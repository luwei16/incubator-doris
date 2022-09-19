
// This file is copied from apache doris

#pragma once
#include <mutex>
#include <vector>

namespace selectdb {
// An ObjectPool maintains a list of C++ objects which are deallocated
// by destroying the pool.
// Thread-safe.
class ObjectPool {
public:
    ObjectPool() = default;
    ~ObjectPool() { clear(); }
    template <class T>
    T* add(T* t) {
        // TODO: Consider using a lock-free structure.
        std::lock_guard l(lock_);
        _objects.emplace_back(Element {t, [](void* obj) { delete reinterpret_cast<T*>(obj); }});
        return t;
    }
    template <class T>
    T* add_array(T* t) {
        std::lock_guard l(lock_);
        _objects.emplace_back(Element {t, [](void* obj) { delete[] reinterpret_cast<T*>(obj); }});
        return t;
    }
    void clear() {
        std::lock_guard l(lock_);
        for (Element& elem : _objects) elem.delete_fn(elem.obj);
        _objects.clear();
    }
    void acquire_data(ObjectPool* src) {
        _objects.insert(_objects.end(), src->_objects.begin(), src->_objects.end());
        src->_objects.clear();
    }
    uint64_t size() {
        std::lock_guard l(lock_);
        return _objects.size();
    }

    ObjectPool(const ObjectPool&) = delete;
    void operator=(const ObjectPool&) = delete;

private:
    /// A generic deletion function pointer. Deletes its first argument.
    using DeleteFn = void (*)(void*);
    /// For each object, a pointer to the object and a function that deletes it.
    struct Element {
        void* obj;
        DeleteFn delete_fn;
    };
    std::vector<Element> _objects;
    std::mutex lock_;
};

} // namespace selectdb
