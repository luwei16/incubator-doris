#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "simple_sync_queue.h"

namespace selectdb {

class SimpleThreadPool {
private:
    using JobType = std::function<void()>;
    // a lock based sync queue
    std::shared_ptr<SimpleSyncQueue<JobType>> _job_queue;

    std::vector<std::thread> _worker_thread_group; // multi thread pool
    std::atomic<bool> _is_running;
    size_t _pool_size;

public:
    SimpleThreadPool(size_t size) : _is_running(false), _pool_size(size) {
        _job_queue = std::make_shared<SimpleSyncQueue<JobType>>(_pool_size * 2);
    }

    SimpleThreadPool() = delete;

    /**
     * Submits jobs, if job queue is full it will blocked
     *
     * @return 0 if succeeded
     */
    int submit(JobType f) {
        if (!_is_running) {
            // not started
            return -1;
        }
        _job_queue->emplace(std::move(f));
        return 0;
    }

    /**
     * Submits jobs with a limit blocking time
     *
     * @param timeout max blocking time in milliseconds 
     * @return 0 if success
     *         -1 if thread pool not start yet
     *         -2 if time out
     */
    int submit_with_timeout(JobType f, int timeout) {
        if (!_is_running) {
            // not started
            return -1;
        }
        if (!_job_queue->emplace_with_timeout(timeout, std::move(f))) {
            return -2;
        }
        return 0;
    }

    /**
     *
     * @return always 0
     */
    int start() {
        _is_running = true;
        _worker_thread_group.clear();
        for (size_t i = 0; i < _pool_size; ++i) {
            _worker_thread_group.emplace_back(&SimpleThreadPool::work, this);
        }
        return 0;
    }

    /**
     * Stops to get jobs from job queue, the job being done will finish normally
     *
     * @return 0 if succeed, otherwise non-zero value returned
     */
    int stop() {
        if (!_is_running) {
            // already stopped
            return -1;
        }
        _is_running = false;
        for (auto& i : _worker_thread_group) {
            if (i.joinable()) {
                i.join();
            }
        }
        return 0;
    }

    /**
     * Gets size of the pool
     *
     * @return the thread number(pool size) in the thread pool
     */
    size_t size() { return _pool_size; }

    /**
     * Terminates the thread pool immediately, jobs in the queue will not be done
     * and the running threads will be detached
     *
     * @return 0 if succeed, otherwise non-zero value returned
     */
    int terminate() {
        if (!_is_running) {
            // not running
            return -1;
        }
        _is_running = false;
        _job_queue->clear(); // abandon all jobs
        for (auto& i : _worker_thread_group) {
            i.detach();
        }
        return 0;
    }

    ~SimpleThreadPool() {
        if (_is_running) {
            stop();
        }
    }

private:
    /**
     * Working thread
     */
    void work() {
        while (_is_running || !_job_queue->empty()) {
            JobType job;
            if (!_job_queue->get_with_timeout(&job, 10)) {
                continue;
            }
            try {
                job();
            } catch (...) {
                // do nothing
            }
        }
    }
};

} // namespace selectdb
