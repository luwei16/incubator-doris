#include "mem_txn_kv.h"
#include <cstdint>
#include <memory>
#include <mutex>
#include <cstring>
#include <string>
#include "txn_kv.h"


namespace selectdb {

int MemTxnKv::init() {
    return 0;
}

int MemTxnKv::create_txn(std::unique_ptr<Transaction>* txn) {
    auto t = new memkv::Transaction(this->shared_from_this());
    txn->reset(t);
    return 0;
}

int MemTxnKv::update(std::map<selectdb::memkv::PutType, std::map<std::string, std::string>>& put_kv,
                    std::vector<std::vector<std::string>>& remove_keys, int64_t* committed_version) {
    std::lock_guard<std::mutex> l(lock_);

    // check remove_keys's range
    for (const auto& vec: remove_keys) {
        if (vec.size() == 2) {
            const std::string& begin_key = vec[0];
            const std::string& end_key = vec[1];
            if (begin_key >= end_key) {
                return -1;
            }
        }
    }

    int64_t version = committed_version_ + 1;
    int16_t seq = 0;
    for (const auto& [put_type, kvs] : put_kv) {
        switch (put_type) {
        case selectdb::memkv::PutType::NORMAL: {
            for (const auto& [key, value] : kvs) {
                mem_kv_[key] = value;
            }
            break;
        }
        case selectdb::memkv::PutType::ATOMIC_VER_KEY: {
            for (const auto& [key, value] : kvs) {
                std::string ver_key(key);
                gen_version_timestamp(version, seq, &ver_key);
                mem_kv_[ver_key] = value;
            }
            break;
        }
        case selectdb::memkv::PutType::ATOMIC_VER_VAL: {
            for (const auto& [key, value] : kvs) {
                std::string ver_val(value);
                gen_version_timestamp(version, seq, &ver_val);
                mem_kv_[key] = ver_val;
            }
            break;
        }
        case selectdb::memkv::PutType::ATOMIC_ADD: {
            for (const auto& [key, value] : kvs) {
                if (mem_kv_.count(key) == 0) {
                    mem_kv_[key] = value;
                } else {
                    std::string org_val = mem_kv_[key];
                    if (org_val.size() != 8) {
                        org_val.resize(8, '\0');
                    }
                    int64_t res = *reinterpret_cast<const int64_t*>(org_val.data()) + std::stoll(value);
                    std::string res_str = std::string(sizeof(res), '\0');
                    std::memcpy(res_str.data(), &res, res_str.size());
                    mem_kv_[key] = res_str;
                }
            }
            break;
        }
        default:
            break;

        }
    }

    for (const auto& vec: remove_keys) {
        if (vec.size() == 1) {
            mem_kv_.erase(vec[0]);
        } else if (vec.size() == 2) {
            const std::string& begin_key = vec[0];
            const std::string& end_key = vec[1];
            auto begin_iter = mem_kv_.lower_bound(begin_key);
            auto end_iter = mem_kv_.lower_bound(end_key);
            mem_kv_.erase(begin_iter, end_iter);
        }
    }
    committed_version_++;
    *committed_version = committed_version_;
    return 0;
}

int MemTxnKv::gen_version_timestamp(int64_t ver, int16_t seq, std::string* str) {
    // Convert litter endian to big endian
    static auto to_big_int64 = [](int64_t v) {
        v = ((v & 0xffffffff00000000) >> 32) | ((v & 0x00000000ffffffff) << 32);
        v = ((v & 0xffff0000ffff0000) >> 16) | ((v & 0x0000ffff0000ffff) << 16);
        v = ((v & 0xff00ff00ff00ff00) >> 8) | ((v & 0x00ff00ff00ff00ff) << 8);
        return v;
    };

    static auto to_big_int16 = [](int16_t v) {
        v = ((v & 0xff00) >> 8) | ((v & 0x00ff) << 8);
        return v;
    };

    ver = to_big_int64(ver);
    seq = to_big_int16(seq);

    int size = str->size();
    str->resize(size + 10, '\0');
    std::memcpy(str->data() + size, &ver, sizeof(ver));
    std::memcpy(str->data() + size + 8, &seq, sizeof(seq));
    return 0;
}

int MemTxnKv::get(std::string_view key, std::string* val, int64_t* version) {
    std::lock_guard<std::mutex> l(lock_);
    auto iter = mem_kv_.find(std::string(key.data(), key.size()));
    if (iter == mem_kv_.end()) { return 1;}
    *val = iter->second;
    *version = committed_version_;
    return 0;
}

int MemTxnKv::get(std::string_view begin, std::string_view end,
            std::unique_ptr<selectdb::RangeGetIterator>* iter, int limit, int64_t* version) {
    std::lock_guard<std::mutex> l(lock_);
    std::vector<std::pair<std::string, std::string>> kv_list;
    std::string begin_k(begin.data(), begin.size());
    std::string end_k(end.data(), end.size());
    if (begin_k >= end_k) {
        std::unique_ptr<RangeGetIterator> ret(new memkv::RangeGetIterator(kv_list, false));
        *(iter) = std::move(ret);
        *version = committed_version_;
        return 0;
    }

    bool use_limit = true;

    if (limit < 0) { return -1;}
    if (limit == 0) { use_limit = false;}

    bool more = false;
    auto begin_iter = mem_kv_.lower_bound(begin_k);
    auto end_iter = mem_kv_.lower_bound(end_k);
    for (; begin_iter != mem_kv_.end() && begin_iter != end_iter; begin_iter++) {
        kv_list.push_back({begin_iter->first, begin_iter->second});

        if (use_limit) {
            limit--;
            if (limit == 0) { break;}
        }
    }
    if (use_limit && limit == 0 && ++begin_iter != end_iter) {
        more = true;
    }
    std::unique_ptr<RangeGetIterator> ret(new memkv::RangeGetIterator(kv_list, more));
    *(iter) = std::move(ret);
    *version = committed_version_;
    return 0;
}

int64_t MemTxnKv::get_last_commited_version() {
    std::lock_guard<std::mutex> l(lock_);
    return committed_version_;
}

int64_t MemTxnKv::get_last_read_version() {
    std::lock_guard<std::mutex> l(lock_);
    read_version_ = committed_version_;
    return read_version_;
}

} // namespace selectdb

namespace selectdb::memkv {

// =============================================================================
// Impl of Trasaction
// =============================================================================

int Transaction::init() {
    return 0;
}

int Transaction::begin() {
    return 0;
}

void Transaction::put(std::string_view key, std::string_view val) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    std::string v(val.data(), val.size());
    put_kv_[PutType::NORMAL][k] = v;
}

int Transaction::get(std::string_view key, std::string* val) {
    return kv_->get(key, val, &read_version_);
}

int Transaction::get(std::string_view begin, std::string_view end,
                     std::unique_ptr<selectdb::RangeGetIterator>* iter, int limit) { 
    return kv_->get(begin, end, iter, limit, &read_version_);
}

void Transaction::atomic_set_ver_key(std::string_view key_prefix, std::string_view val) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key_prefix.data(), key_prefix.size());
    std::string v(val.data(), val.size());
    put_kv_[PutType::ATOMIC_VER_KEY][k] = v;
}

void Transaction::atomic_set_ver_value(std::string_view key, std::string_view value) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    std::string v(value.data(), value.size());
    put_kv_[PutType::ATOMIC_VER_VAL][k] = v;
}

void Transaction::atomic_add(std::string_view key, int64_t to_add) {
    std::lock_guard<std::mutex> l(lock_);
    put_kv_[PutType::ATOMIC_ADD][std::string(key.data(), key.size())] = std::to_string(to_add);
}

void Transaction::remove(std::string_view key) {
    std::lock_guard<std::mutex> l(lock_);
    remove_keys_.push_back({std::string(key.data(), key.size())});
}

void Transaction::remove(std::string_view begin, std::string_view end) {
    std::lock_guard<std::mutex> l(lock_);
    std::string begin_k(begin.data(), begin.size());
    std::string end_k(end.data(), end.size());
    remove_keys_.push_back({begin_k, end_k});
}

int Transaction::commit() {
    std::lock_guard<std::mutex> l(lock_);
    int ret = kv_->update(put_kv_, remove_keys_, &committed_version_);
    if (ret != 0) {
        return -2;
    }
    commited_ = true;
    put_kv_.clear();
    remove_keys_.clear();
    return 0;
}

int64_t Transaction::get_read_version() {
    return read_version_;
}

int64_t Transaction::get_committed_version() {
    return committed_version_;
}

int Transaction::abort() {
    return 0;
}

} // namespace selectdb::memkv