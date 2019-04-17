// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <unistd.h>

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <iostream>
#include <fstream>

#include <algorithm>
#include <string>
#include <set>
#include <queue>
#include <ctime>
#include <vector>
#include <unordered_set>
#include <unordered_map>

#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

#include "include/engine.h"

namespace polar_race {

struct Item {
	size_t p;
	unsigned szKey, szVal;
};

unsigned long long hashPolar(const char* s, int n);

class EngineRace : public Engine  {
public:
	struct DataBlk {
		char *pmem;
		std::mutex* op;
		std::atomic<int>* usecnt;
		clock_t ts;

		DataBlk(char* _pmem=0) : pmem(_pmem), usecnt(0), ts(0) {
			op = new std::mutex();
		}
		~DataBlk() {
			// delete op;
		}
	};
private:
	static const size_t max_journal = 6;
	static const size_t chunk_size = 64 << 20; 
	static const size_t max_chunks = (4ul << 30) / chunk_size;

	size_t n_items, n_journal, p_synced, p_current, sz_current, sz_synced;
	size_t fsz;
	size_t loaded_size, last_chunk_sz;
	size_t* idxs;

	Item* journal;
	std::mutex* ready;
	std::vector<Item> meta; 
	std::vector<DataBlk> datablks; 

	std::unordered_map<std::string, size_t> lookup_long;
	std::unordered_map<unsigned long long, size_t> lookup_short;

	std::mutex journal_mtx;

	std::mutex ret_mtx;
	std::condition_variable ret_cv;

	std::ofstream ou_meta;

	int fd;
	char* p_disk;
	
	bool alive;
	bool flushing;
	std::thread* p_daemon;

public:
	static RetCode Open(const std::string& name, Engine** eptr);

	explicit EngineRace(const std::string& dir) : n_journal(0) {
		journal = new Item[max_journal];
		idxs = new size_t[max_journal];
		ready = new std::mutex[max_journal];
	}

	~EngineRace();

	RetCode Write(const PolarString& key,
			const PolarString& value) override;

	RetCode Read(const PolarString& key,
			std::string* value) override;

	/*
	 * NOTICE: Implement 'Range' in quarter-final,
	 *         you can skip it in preliminary.
	 */
	RetCode Range(const PolarString& lower,
			const PolarString& upper,
			Visitor &visitor) override;

	void init(const std::string&);

private: 
	size_t allocMemory(size_t);

	inline char* getDiskPtr(size_t blk) {
		return p_disk + blk * chunk_size;
	}

	inline char* getPtrSafe(size_t blk) {
		++datablks[blk].usecnt;
		if (datablks[blk].pmem == 0) {
			datablks[blk].op->lock();
			if (datablks[blk].pmem == 0) {
				datablks[blk].pmem = new char[chunk_size];
				datablks[blk].ts = clock();
				memcpy(datablks[blk].pmem, getDiskPtr(blk), chunk_size);
			}
			datablks[blk].op->unlock();
		}
		return datablks[blk].pmem;
	}

	inline char* getMemory(size_t ptr, bool safe=false) {
		size_t blk(ptr / chunk_size), p(ptr % chunk_size);
		if (safe) {
			return getPtrSafe(blk) + p;
		} else {
			return datablks[blk].pmem + p;
		}
	}

	inline void relieveMemory(size_t ptr) {
		size_t blk(ptr / chunk_size);
		--datablks[blk].usecnt;
	}

	void copyToMemory(size_t, size_t, const PolarString&, const PolarString&);
	void flush();
	size_t find(const PolarString& key);
	void daemon();
	size_t recycleMemory();
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
