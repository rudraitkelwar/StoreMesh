#pragma once
/**
 * thread_pool.hpp — StoreMesh
 *
 * A fixed-size thread pool built on std::thread, std::mutex, and
 * std::condition_variable.  Workers sleep until a task arrives, then
 * wake one thread via notify_one().  The caller gets a std::future<T>
 * so it can block on the result without polling.
 *
 * WHY THIS MATTERS FOR INTERVIEWS:
 *   Pure Storage interviewers ask about concurrency primitives.  This
 *   shows you understand: thread lifecycle, condition_variable spurious
 *   wakeups (hence the predicate lambda), packaged_task / future
 *   ownership, and graceful shutdown ordering (set stop_ BEFORE
 *   notify_all so workers see the flag).
 */

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads) : stop_(false) {
        workers_.reserve(num_threads);
        for (std::size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~ThreadPool() {
        // Signal all workers to drain and exit
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
    }

    // Disable copy and move — owning live threads is not copyable
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    /**
     * Enqueue a callable and return a future for its return value.
     * The callable runs on one of the pool threads.
     *
     * Usage:
     *   auto fut = pool.enqueue([](int x){ return x * 2; }, 21);
     *   int result = fut.get(); // == 42
     */
    template <class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<typename std::invoke_result<F, Args...>::type>
    {
        using R = typename std::invoke_result<F, Args...>::type;

        // packaged_task wraps the callable so we can extract a future
        auto task = std::make_shared<std::packaged_task<R()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<R> result = task->get_future();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (stop_) throw std::runtime_error("enqueue on stopped ThreadPool");
            // Wrap packaged_task in a type-erased std::function<void()>
            tasks_.push([task]() { (*task)(); });
        }
        cv_.notify_one(); // wake exactly one sleeping worker
        return result;
    }

    std::size_t thread_count() const { return workers_.size(); }

    std::size_t pending_tasks() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return tasks_.size();
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                // Wait until there is work OR we are stopping.
                // The predicate prevents spurious wakeups from dropping tasks.
                cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });

                if (stop_ && tasks_.empty()) return; // clean exit

                task = std::move(tasks_.front());
                tasks_.pop();
            }
            task(); // execute outside the lock so other workers can pick up work
        }
    }

    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> tasks_;
    mutable std::mutex                mutex_;
    std::condition_variable           cv_;
    bool                              stop_;
};
