#pragma once
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>

class ThreadPool 
{
    public:
        explicit ThreadPool(std::size_t num_threads) : stop_(false)
        {
            for(std::size_t i = 0; i < num_threads; i++)
            {
                workers_.emplace_back([this]
                {
                    while(true)
                    {
                        std::function<void()> task;
                        {
                            std::unique_lock<std::mutex> lock(mutex_);
                            cv_.wait(lock, [this]
                            {
                                return stop_ || !tasks_.empty();
                            });

                            if(stop_ && tasks_.empty()) return;

                            task = std::move(tasks_.front());
                            tasks_.pop();
                        }
                        task();
                    }
                });
            }
        }

        ~ThreadPool()
        {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                stop_ = true;
            }
            cv_.notify_all();
            for(auto& t : workers_)
            {
                if(t.joinable()) t.join();
            }
        }
        template<class F, class... Args>
        auto enqueue(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F, Args...>::type>
        {
            using R = typename std::invoke_result<F, Args...>::type;

            auto bound_function = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

            auto task = std::make_shared<std::packaged_task<R()>>(bound_function);

            std::future<R> result = task->get_future();
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if(stop_) throw std::runtime_error("enqueue on stopped threadpool");
                tasks_.push([task]() {(*task)();});
            }
            cv_.notify_one();
            return result;
        }

    private:
        std::vector<std::thread> workers_;
        std::queue<std::function<void()>> tasks_;
        std::mutex mutex_;
        std::condition_variable cv_;
        bool stop_; 
};