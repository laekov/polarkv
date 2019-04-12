// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

namespace polar_race {

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);

  *eptr = engine_race;
  return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
	journal_mtx.lock();
	flush();
	journal_mtx.unlock();
	if (datablks.size()) {
		if (loaded_size) {
			delete [] datablks[0];
		}
		for (size_t i = loaded_size; i < datablks.size(); ++i) {
			delete [] datablks[i];
		}
	}
	datablks.resize(0);
	alive = false;
	this->p_daemon->join();
	delete [] journal;
	delete [] ready;
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
	journal_mtx.lock();
	size_t idx(n_journal++);
	journal[idx].szKey = key.size();
	journal[idx].szVal = value.size();
	journal[idx].p = this->allocMemory(key.size() + value.size());

	if (idx < max_journal) {
		ready[idx].lock();
		journal_mtx.unlock();
		this->copyToMemory(journal[idx].p, key, value);
		ready[idx].unlock();

		std::unique_lock<std::mutex> lck(ret_mtx);
		ret_cv.wait(lck);
	} else {
		this->copyToMemory(journal[idx].p, key, value);
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
		char* ptr(new char[chunk_size]);
		datablks.push_back(ptr);
		sz_current = 0;
	}
	size_t res(p_current * chunk_size + sz_current);
	sz_current += totsz;
	return res;
}

void EngineRace::copyToMemory(size_t ptr, const PolarString& key, const PolarString& value) {
	char* d(getMemory(ptr));
	memcpy(d, key.data(), key.size());
	memcpy(d + key.size(), value.data(), value.size());
}

void EngineRace::flush() {
	static const size_t blk_upd_chk = 5;
	std::set<size_t> blk_to_upd;
	std::vector<size_t> idxs(n_journal);
#pragma omp parallel for
	for (size_t i = 0; i < n_journal; ++i) {
		ready[i].lock();
		ready[i].unlock();
		size_t idx(find(PolarString(getMemory(journal[i].p), journal[i].szKey)));
		idxs[i] = idx;
	}
	for (size_t i = 0; i < n_journal; ++i) {
		size_t idx(idxs[i]);
		if (idx == -1u) {
			idx = meta.size();
			lookup[PolarString(getMemory(journal[i].p), journal[i].szKey)] = idx;
			meta.push_back(journal[i]);
		} else {
			meta[idx] = journal[i];
		}
		blk_to_upd.insert(idx >> blk_upd_chk);
	}
	for (size_t p : blk_to_upd) {
		p <<= blk_upd_chk;
		ou_meta.seekp(p * sizeof(Item));
		ou_meta.write((char*)meta.data() + p * sizeof(Item),
				      std::min(meta.size() - p, (size_t)(1lu << blk_upd_chk)) * sizeof(Item));
	}
	ou_meta.flush();

	ou_data.seekp(p_synced * chunk_size);
	for (size_t i = p_synced; i < p_current; ++i) {
		ou_data.write(datablks[i], chunk_size);
	}
	if (p_synced == p_current) {
		if (sz_current > sz_synced) {
			ou_data.seekp(p_synced * chunk_size + sz_synced);
			ou_data.write(datablks[p_synced] + sz_synced, sz_current - sz_synced);
		}
	} else if (sz_current > 0) {
		ou_data.write(datablks[p_current], sz_current);
	}
	ou_data.flush();
	p_synced = p_current;
	sz_synced = sz_current;
	n_journal = 0;
	ret_cv.notify_all();
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
	size_t idx(find(key));
	if (idx != -1u) {
		value->resize(meta[idx].szVal);
		memcpy((char*)value->data(), getMemory(meta[idx].p + meta[idx].szKey), meta[idx].szVal);
		return kSucc;
	} else {
		return kNotFound;
	}
}

size_t EngineRace::find(const PolarString& key) {
	auto it(lookup.find(key));
	if (it == lookup.end()) {
		return -1u;
	} else {
		return it->second;
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

void EngineRace::daemon() {
	while (alive) {
		journal_mtx.lock();
		flush();
		journal_mtx.unlock();
		usleep(10);
	}
}

}  // namespace polar_race
