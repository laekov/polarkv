// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

namespace polar_race {

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

unsigned long long hashPolar(const char* s, int n) {
	unsigned long long a(0);
	for (int i = 0; i < n; ++i) {
		a = ((a << 8) | s[i]);
	}
	return a;
}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);
  engine_race->init(name);
  *eptr = engine_race;
  return kSucc;
}

void EngineRace::init(const std::string& name) {
	std::ifstream meta_in(name + ".meta", std::ios::binary);
	if (meta_in.is_open()) {
		meta_in.seekg(0, meta_in.end);
		n_items = meta_in.tellg() / sizeof(Item);
		meta_in.seekg(0, meta_in.beg);
		meta.resize(n_items);
		meta_in.read((char*)meta.data(), n_items * sizeof(Item));
		meta_in.close();
	} else {
		n_items = 0;
	}

	fd = open((name + ".data").c_str(), O_CREAT | O_RDWR | O_NOATIME, 0644);
	if (fd == -1) {
		fprintf(stderr, "Error %d\n", errno);
	}
	if (meta.size()) {

		std::ifstream data_in(name + ".data", std::ios::binary);
		data_in.seekg(0, data_in.end);
		fsz = data_in.tellg();
		data_in.close();

		if (fsz & (chunk_size - 1)) {
			fsz = (fsz & chunk_size) + chunk_size;
			ftruncate(fd, fsz);
		}

		p_disk = (char*)mmap(0, fsz, PROT_READ, MAP_SHARED, fd, 0);

		for (size_t offset = 0; offset < fsz; offset += chunk_size) {
			datablks.push_back(DataBlk());
		}
		loaded_size = datablks.size();
		p_synced = p_current = datablks.size();
	} else {
		p_disk = 0;
		fsz = 0;
		p_synced = p_current = 0;
	}

	for (size_t i = 0; i < meta.size(); ++i) {
		if (meta[i].szKey > 8) {
			lookup_long[std::string(getMemory(meta[i].p), meta[i].szKey)] = i;
		} else {
			lookup_short[hashPolar(getMemory(meta[i].p), meta[i].szKey)] = i;
		}
	}

	sz_current = 0;
	sz_synced = 0;

	ou_meta.open(name + ".meta", std::ios::binary);

	alive = true;
	flushing = false;
	this->p_daemon = new std::thread(&EngineRace::daemon, this);
	this->p_recyc = new std::thread(&EngineRace::recycle, this);
	this->p_monitor = new std::thread(&EngineRace::monitor, this);
}

// 2. Close engine
EngineRace::~EngineRace() {
	journal_mtx.lock();
	flush();
	journal_mtx.unlock();

	alive = false;
	this->p_daemon->join();
	this->p_recyc->join();
	this->p_monitor->join();

	for (auto& b : datablks) {
		if (b.pmem) {
			delete [] b.pmem;
		}
	}
	datablks.resize(0);

	delete [] journal;
	delete [] idxs;
	delete [] ready;

	if (p_disk) {
		munmap(p_disk, fsz);
	}
	close(fd);
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
	journal_mtx.lock();
	size_t idx(n_journal++);
	journal[idx].szKey = key.size();
	journal[idx].szVal = value.size();
	journal[idx].p = this->allocMemory(key.size() + value.size());

	if (n_journal < max_journal) {
		ready[idx].lock();
		journal_mtx.unlock();
		this->copyToMemory(idx, journal[idx].p, key, value);
		ready[idx].unlock();

		std::unique_lock<std::mutex> lck(ret_mtx);
		ret_cv.wait(lck);
	} else {
		this->copyToMemory(idx, journal[idx].p, key, value);
		this->flush();
		journal_mtx.unlock();
	}
	return kSucc;
}

size_t EngineRace::allocMemory(size_t totsz) {
	if (sz_current + totsz > chunk_size) {
		++p_current;
	}
	if (datablks.size() <= p_current) {
		DataBlk ptr(new char[chunk_size]);
		datablks.push_back(ptr);
		sz_current = 0;
	}
	size_t res(p_current * chunk_size + sz_current);
	sz_current += totsz;
	return res;
}

void EngineRace::copyToMemory(size_t idx, size_t ptr, const PolarString& key, const PolarString& value) {
	idxs[idx] = find(key);
	char* d(getMemory(ptr));
	memcpy(d, key.data(), key.size());
	memcpy(d + key.size(), value.data(), value.size());
}

void EngineRace::flush() {
	flushing = true;
	static const size_t blk_upd_chk = 5;
	std::unordered_set<size_t> blk_to_upd;

	for (size_t i = 0; i < n_journal; ++i) {
		ready[i].lock();
		ready[i].unlock();
		size_t idx(idxs[i]);
		if (idx == -1u) {
			idx = meta.size();
			if (journal[i].szKey > 8) {
				lookup_long[std::string(getMemory(journal[i].p), journal[i].szKey)] = idx;
			} else {
				lookup_short[hashPolar(getMemory(journal[i].p), journal[i].szKey)] = idx;
			}
			meta.push_back(journal[i]);
		} else {
			meta[idx] = journal[i];
		}

		blk_to_upd.insert(idx >> blk_upd_chk);
	}

	if (fsz < (p_current + 1) * chunk_size) {
        auto old_fsz(fsz);
		if (fsz == 0) {
			fsz = (p_current + 1) * chunk_size;
		} else {
			fsz = fsz * 2;
		}
		ftruncate(fd, fsz);
		char* new_p_disk = (char*)mmap(0, fsz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		if (p_disk != 0) {
            p_disk_mtx.lock();
			munmap(p_disk, old_fsz);
            p_disk = new_p_disk;
            p_disk_mtx.unlock();
        } else {
            p_disk = new_p_disk;
        }
    }

	for (size_t i = p_synced; i < p_current; ++i) {
		memcpy(getDiskPtr(i), datablks[i].pmem, chunk_size);
	}
	if (p_synced == p_current) {
		if (sz_current > sz_synced) {
            char* pdisk(getDiskPtr(p_synced) + sz_synced);
            char* p_mem(datablks[p_synced].pmem + sz_synced);
			memcpy(pdisk, p_mem, sz_current - sz_synced); 
		}
	} else if (sz_current > 0) {
		memcpy(getDiskPtr(p_current), datablks[p_current].pmem, sz_current);
	}

	for (size_t p : blk_to_upd) {
		p <<= blk_upd_chk;
		ou_meta.seekp(p * sizeof(Item));
		ou_meta.write((char*)meta.data() + p * sizeof(Item),
				std::min(meta.size() - p, (size_t)(1lu << blk_upd_chk)) * sizeof(Item));
	}
	ou_meta.flush();

	p_synced = p_current;
	sz_synced = sz_current;
    n_ops += n_journal;
	n_journal = 0;
	ret_cv.notify_all();
	flushing = false;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
	size_t idx(find(key));
	if (idx != -1u) {
		value->resize(meta[idx].szVal);
        size_t ptr(meta[idx].p + meta[idx].szKey);
        char* dataptr(getMemory(ptr, true));
		memcpy((char*)value->data(), dataptr, meta[idx].szVal);
        relieveMemory(ptr);
        return kSucc;
	} else {
		return kNotFound;
	}
}

template<class T>
size_t t_find(std::unordered_map<T, size_t>& lookup, T key) {
	auto it(lookup.find(key));
	if (it == lookup.end()) {
		return -1u;
	} else {
		return it->second;
	}
}

size_t EngineRace::find(const PolarString& key) {
	if (key.size() > 8) {
		return t_find(lookup_long, key.ToString());
	} else {
		return t_find(lookup_short, hashPolar(key.data(), key.size()));
	}
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
		Visitor &visitor) {
	return kSucc;
}

size_t EngineRace::recycleMemory() {
	size_t n = p_synced;
	std::vector<std::pair<clock_t, size_t> > clks;
	for (size_t i = 0; i < n; ++i) {
		if (datablks[i].pmem) {
			clks.push_back(std::pair<clock_t, size_t>(datablks[i].ts, i));
		}
	}
	if (clks.size() > max_chunks) {
		size_t m = clks.size() - max_chunks, recycled(0);
        std::sort(clks.begin(), clks.end());
		for (size_t i = 0; i < n && recycled < m; ++i) {
			size_t j = clks[i].second;
			datablks[j].op->lock();
			if (datablks[j].usecnt == 0) {
				delete [] datablks[j].pmem;
				datablks[j].pmem = 0;
				++recycled;
			}
			datablks[j].op->unlock();
		}
		return recycled;
	}
	return 0;
}

void EngineRace::daemon() {
	size_t interval(1 << 3);
	size_t mem_recycled(0);
	while (alive) {
		if (!flushing && n_journal > 0) {
			if (interval > 1) {
				interval >>= 1;
			}
		} else {
			if (interval < (1 << 10)) {
				interval <<= 1;
			}
		}
		if (n_journal > 0) {
			journal_mtx.lock();
			flush();
			journal_mtx.unlock();
		}
		usleep(interval);
	}
}

void EngineRace::recycle() {
	size_t mem_recycled(0);
    while (alive) {
		mem_recycled = recycleMemory();
        usleep(500000);
    }
}

void EngineRace::monitor() {
    size_t last_ops(0);
    size_t last_loads(0);
    while (alive) {
        size_t activeblk(0);
        for (auto& i : datablks) {
            if (i.pmem) {
                ++activeblk;
                // fprintf(stderr, "%3d", i.usecnt);
            } else {
                //fprintf(stderr, "---");
            }
        }
        n_ops += n_journal;
        fprintf(stderr, "SS %lu SL %lu ", lookup_short.size(), lookup_long.size());
        fprintf(stderr, " %lu / %lu blks %lu lps %lu datas %lu wps\n", 
                activeblk, datablks.size(),
                n_load - last_loads,
                n_ops,
                n_ops - last_ops);
        last_ops = n_ops;
        last_loads = n_load;
        sleep(1);
    }
}

char* EngineRace::getPtrSafe(size_t blk, bool safe) {
    if (safe) {
        char* ptr;
        datablks[blk].op->lock();
        ++datablks[blk].usecnt;
        ptr = datablks[blk].pmem;
        datablks[blk].op->unlock();
        if (ptr == 0) {
            datablks[blk].op->lock();
            if (datablks[blk].pmem == 0) {
                datablks[blk].pmem = new char[chunk_size];
                p_disk_mtx.lock();
                ++n_load;
                memcpy(datablks[blk].pmem, getDiskPtr(blk), chunk_size);
                p_disk_mtx.unlock();
            }
            datablks[blk].op->unlock();
        }
    } else if (datablks[blk].pmem == 0) {
        datablks[blk].pmem = new char[chunk_size];
        memcpy(datablks[blk].pmem, getDiskPtr(blk), chunk_size);
    }
    datablks[blk].ts = clock();
    return datablks[blk].pmem;
}

}  // namespace polar_race
