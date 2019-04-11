// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <fstream>
#include <string>
#include <mutex>
#include <condition_variable>
#include "include/engine.h"

namespace polar_race {

struct Item {
	size_t p;
	unsigned szKey, szVal;
};

class EngineRace : public Engine  {
public:
	static const size_t max_journal = 1 << 10;
	static const size_t buffer_size = 32 << 20; // 32MB

	size_t n_items, n_journal, p_synced, p_current;
	Item* journal;
	std::mutex* ready;
	vector<Item> meta; 
	vector<char*> datablks; 

	std::mutex journal_mtx;

	std::mutex ret_mtx;
	std::condition_variable ret_cv;


public:
	static RetCode Open(const std::string& name, Engine** eptr);

	explicit EngineRace(const std::string& dir) {
		journal = new Item[max_journal];
		ready = new std::mutex[max_journal];

		n_journal = 0;

		std::ifstream meta_in(dir + ".meta", std::ios::binary);
		if (meta_in.is_open()) {
			meta_in.seekg(0, meta_in.end);
			n_items = meta_in.tellg() / sizeof(Item);
			meta_in.seekg(0, meta_in.beg);
			metablks.resize(n_items);
			meta_in.read(meta.data(), n_items * sizeof(Item));
			meta_in.close();
		} else {
			n_items = 0;
		}

		if (metablks.size()) {
			std::ifstream data_in(dir + ".data", std::ios::binary);
			data_in.seekg(0, data_in.end);
			size_t fsz(data_in.tellg());
			data_in.seekg(0, data_in.beg);
			char* rd_buf = new char[fsz];
			data_in.read(rd_buf, fsz);
			data_in.close();
			for (size_t offset = 0; offset < fsz; offset += buffer_size) {
				datablks.push_back(rd_buf + offset);
			}
			p_synced = p_current = datablks.size();
		} else {
			p_synced = p_current = 0;
		}
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

private: 

};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
