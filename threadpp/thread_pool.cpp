#include "thread_pool.h"

#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace tpp
{

class thread_pool::impl
{
    struct job_handle
    {
        job_id id = 0;
        priority::group group;
        std::string name;
    };

    struct job_info
    {
        job_handle handle;

        task callable;
        shared_future<void> callable_future;
    };

    friend bool operator<(const job_handle& lhs, const job_handle& rhs)
    {
        return lhs.group.priority < rhs.group.priority;
    }

    using workers = std::vector<tpp::thread>;
    using priority_workers = std::map<priority::category, workers>;
    using jobs_queue = std::priority_queue<job_handle>;
    using priority_queues = std::map<priority::category, jobs_queue>;

public:
    impl(const std::map<priority::category, size_t>& workers_per_priority_level, tasks_capacity_config config)
    {
        jobs_.reserve(config.default_reserved_tasks);
        for(const auto& kvp : workers_per_priority_level)
        {
            auto level = kvp.first;
            auto count = kvp.second;
            if(count > 0)
            {
                job_priority_queues_[level];
                auto& workers_for_level = workers_[level];
                workers_for_level.reserve(count);
                for(size_t i = 0; i < count; ++i)
                {
                    std::string name = "pool worker:" + std::to_string(unsigned(level)) + ":" + std::to_string(i);
                    workers_for_level.emplace_back(make_thread(name));
                    auto& task = workers_for_level.back();
                    tpp::set_thread_config(task.get_id(), config);
                }
            }
        }
    }

    impl(impl&&) = delete;
    impl& operator=(impl&&) = delete;
    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;

    ~impl()
    {
        clear_all();

        auto workers = [&]()
        {
            std::lock_guard<std::mutex> lock(guard_);
            return std::move(workers_);
        }();

        workers.clear();
    }

    auto add_job(task& user_job, priority::group group, const std::string& name) -> job_id
    {
        auto packaged_task = detail::package_future_task(std::move(user_job));
        std::lock_guard<std::mutex> lock(guard_);
        auto id = free_id_++;
        auto& job = jobs_[id];
        job.handle.id = id;
        job.handle.group = group;
        job.handle.name = name;
        job.callable = std::move(packaged_task.callable);
        job.callable_future = packaged_task.callable_future.share();

        add_job_handle(job.handle);
        return id;
    }

    void change_priority(job_id id, priority::group group)
    {
        std::lock_guard<std::mutex> lock(guard_);

        auto it = jobs_.find(id);
        if(it == jobs_.end())
        {
            return;
        }

        job_info& job = it->second;

        if(!job.callable || job.handle.group == group)
        {
            return;
        }

        job.handle.group = group;

        add_job_handle(job.handle);
    }

    void clear(job_id id, bool check_callable)
    {
        std::lock_guard<std::mutex> lock(guard_);
        auto it = jobs_.find(id);
        if(it != jobs_.end())
        {
            if(check_callable)
            {
                if(it->second.callable)
                {
                    jobs_.erase(id);
                }
            }
            else
            {
                jobs_.erase(id);
            }
        }
    }

    void clear_all()
    {
        std::lock_guard<std::mutex> lock(guard_);
        jobs_.clear();
        job_priority_queues_.clear();
    }

    void wait(job_id id)
    {
        auto f = [this, id]()
        {
            std::lock_guard<std::mutex> lock(guard_);
            auto it = jobs_.find(id);
            if(it == jobs_.end())
            {
                return make_ready_future().share();
            }
            return it->second.callable_future;
        }();

        f.wait();
    }

    void wait_all()
    {
        std::vector<shared_future<void>> futures;
        {
            std::lock_guard<std::mutex> lock(guard_);
            futures.reserve(jobs_.size());

            for(const auto& jobkvp : jobs_)
            {
                auto& job = jobkvp.second;
                futures.emplace_back(job.callable_future);
            }
        }

        for(const auto& future : futures)
        {
            future.wait();
        }
    }

    void wait_all(priority::category category, const on_progress_callback& on_progress)
    {
        
        struct job_info_wrapper
        {
            job_handle handle;
            shared_future<void> future;
        };

        std::vector<job_info_wrapper> futures;
        {
            std::lock_guard<std::mutex> lock(guard_);
            futures.reserve(jobs_.size());
            for(const auto& kvp : jobs_)
            {
                auto& job = kvp.second;
                if(job.handle.group.level >= category)
                {
                    job_info_wrapper wrapper;
                    wrapper.handle = job.handle;
                    wrapper.future = job.callable_future;
                    futures.emplace_back(std::move(wrapper));
                }
            }
        }
        size_t current_job = 0;
        for(const auto& wrapper : futures)
        {
            wrapper.future.wait();
            current_job++;
            if(on_progress)
            {
                progress_info info;
                info.name = wrapper.handle.name;
                info.current_job = current_job;
                info.total_jobs = futures.size();
                on_progress(info);
            }
        }
    }

    auto get_jobs_count() const -> size_t
    {
        std::lock_guard<std::mutex> lock(guard_);
        return jobs_.size();
    }

    auto get_jobs_count_detailed() const -> std::map<std::string, size_t>
    {
        std::lock_guard<std::mutex> lock(guard_);
        std::map<std::string, size_t> result;
        for(const auto& kvp : jobs_)
        {
            result[kvp.second.handle.name]++;
        }
        return result;
    }

private:
    void add_job_handle(job_handle handle)
    {
        job_priority_queues_[handle.group.level].emplace(handle);
        notify_workers(handle.group.level);
    }

    void notify_workers(priority::category max_priority)
    {
        for(const auto& kvp : workers_)
        {
            auto priority = kvp.first;

            if(priority <= max_priority)
            {
                auto& workers = workers_[priority];
                for(auto& w : workers)
                {
                    invoke(w.get_id(),
                           [this, priority]()
                           {
                               check_jobs(priority);
                           });
                }
            }
        }
    }

    auto get_highest_priority_queue_above(priority::category level) -> jobs_queue&
    {
        priority::category selected_level = level;

        for(const auto& kvp : job_priority_queues_)
        {
            auto queue_priority_level = kvp.first;

            if(selected_level <= queue_priority_level)
            {
                auto& job_queue = kvp.second;

                if(!job_queue.empty())
                {
                    selected_level = queue_priority_level;
                }
            }
        }
        return job_priority_queues_[selected_level];
    }

    void check_jobs(priority::category level)
    {
        if(this_thread::notified_for_exit())
        {
            return;
        }

        task user_job;
        job_id id = 0;

        {
            std::lock_guard<std::mutex> lock(guard_);
            auto& job_queue = get_highest_priority_queue_above(level);

            if(job_queue.empty())
            {
                return;
            }
            const auto& handle = job_queue.top();
            auto it = jobs_.find(handle.id);
            if(it == jobs_.end())
            {
                job_queue.pop();
                return;
            }
            auto& job = it->second;

            // if priority level is lower still matches
            if(level <= job.handle.group.level)
            {
                id = job.handle.id;
                user_job = std::move(job.callable);
            }

            job_queue.pop();
        }
        ////////////
        if(user_job)
        {
            user_job();
            // clear after the call so that the task
            // is waitable via the pool.
            clear(id, false);
        }
    }

    mutable std::mutex guard_;
    job_id free_id_ = 1;
    priority_workers workers_;
    std::unordered_map<job_id, job_info> jobs_;
    priority_queues job_priority_queues_;
};

////////////////////////////////////////////////////////////
thread_pool::thread_pool() : thread_pool({{priority::category::low, thread::hardware_concurrency()}})
{
}
thread_pool::thread_pool(const std::map<priority::category, size_t>& workers_per_priority_level,
                         tasks_capacity_config config)
{
    impl_ = std::make_unique<impl>(workers_per_priority_level, config);
}

thread_pool::~thread_pool() = default;

job_id thread_pool::add_job(task& job, priority::group group, const std::string& name)
{
    return impl_->add_job(job, group, name);
}

void thread_pool::change_priority(job_id id, priority::group group)
{
    impl_->change_priority(id, group);
}

void thread_pool::stop_all()
{
    impl_->clear_all();
}

void thread_pool::wait(job_id id)
{
    impl_->wait(id);
}

void thread_pool::stop(job_id id)
{
    impl_->clear(id, true);
}

void thread_pool::wait_all()
{
    impl_->wait_all();
}

void thread_pool::wait_all(priority::category category, const on_progress_callback& on_progress)
{
    impl_->wait_all(category, on_progress);
}

size_t thread_pool::get_jobs_count() const
{
    return impl_->get_jobs_count();
}

std::map<std::string, size_t> thread_pool::get_jobs_count_detailed() const
{
    return impl_->get_jobs_count_detailed();
}

void job_future_storage::change_priority(priority::group group)
{
    if(sentinel_.expired())
    {
        return;
    }

    if(owner_)
    {
        owner_->change_priority(id, group);
    }
}

void job_future_storage::stop()
{
    if(sentinel_.expired())
    {
        return;
    }

    if(owner_)
    {
        owner_->stop(id);
    }
}

} // namespace tpp
