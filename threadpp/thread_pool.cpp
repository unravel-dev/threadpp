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
                pending_wake_[level] = false;
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

    auto add_job(task& user_job, priority::group group, const std::string& name, bool queue = true) -> job_id
    {
        auto packaged_task = detail::package_future_task(std::move(user_job));
        std::vector<worker_wakeup> wakeups;
        job_id id = 0;
        {
            std::lock_guard<std::mutex> lock(guard_);
            id = free_id_++;
            auto& target = queue ? jobs_ : deferred_jobs_;
            auto& job = target[id];
            job.handle.id = id;
            job.handle.group = group;
            job.handle.name = name;
            job.callable = std::move(packaged_task.callable);
            job.callable_future = packaged_task.callable_future.share();
            if(queue)
            {
                queue_job_handle(job.handle, wakeups);
            }
        }
        dispatch_wakeups(wakeups);
        return id;
    }

    auto submit(job_id id) -> bool
    {
        std::vector<worker_wakeup> wakeups;
        {
            std::lock_guard<std::mutex> lock(guard_);
            auto it = deferred_jobs_.find(id);
            if(it == deferred_jobs_.end())
            {
                return false;
            }
            auto& job = jobs_[id];
            job = std::move(it->second);
            deferred_jobs_.erase(it);
            queue_job_handle(job.handle, wakeups);
        }
        dispatch_wakeups(wakeups);
        return true;
    }

    void change_priority(job_id id, priority::group group)
    {
        std::vector<worker_wakeup> wakeups;
        {
            std::lock_guard<std::mutex> lock(guard_);
            auto it = jobs_.find(id);
            if(it != jobs_.end())
            {
                job_info& job = it->second;
                if(!job.callable || job.handle.group == group)
                {
                    return;
                }
                job.handle.group = group;
                queue_job_handle(job.handle, wakeups);
            }
            else
            {
                auto dit = deferred_jobs_.find(id);
                if(dit != deferred_jobs_.end())
                {
                    dit->second.handle.group = group;
                }
            }
        }
        dispatch_wakeups(wakeups);
    }

    void clear(job_id id, bool check_callable)
    {
        std::lock_guard<std::mutex> lock(guard_);
        auto it = jobs_.find(id);
        if(it != jobs_.end())
        {
            if(!check_callable || it->second.callable)
            {
                jobs_.erase(it);
            }
            return;
        }

        auto dit = deferred_jobs_.find(id);
        if(dit != deferred_jobs_.end())
        {
            if(!check_callable || dit->second.callable)
            {
                deferred_jobs_.erase(dit);
            }
        }
    }

    void clear_all()
    {
        std::lock_guard<std::mutex> lock(guard_);
        jobs_.clear();
        deferred_jobs_.clear();
        job_priority_queues_.clear();
        for(auto& kvp : pending_wake_)
        {
            kvp.second = false;
        }
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

    void wait_all_polling(priority::category category, const on_progress_callback& on_progress)
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
            while(!wrapper.future.is_ready())
            {
                wrapper.future.wait_for(std::chrono::milliseconds(16));
                if(on_progress)
                {
                    progress_info info;
                    info.name = wrapper.handle.name;
                    info.current_job = current_job;
                    info.total_jobs = futures.size();
                    on_progress(info);
                }
            }

            current_job++;
        }
    }


    auto get_jobs_count() const -> size_t
    {
        std::lock_guard<std::mutex> lock(guard_);
        return jobs_.size();
    }
    auto get_jobs_count(priority::category category) const -> size_t
    {
        std::lock_guard<std::mutex> lock(guard_);
        size_t count = 0;
        for(const auto& kvp : jobs_)
        {
            if(kvp.second.handle.group.level >= category)
            {
                count++;
            }
        }
        return count;
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
    struct worker_wakeup
    {
        thread::id id{};
        priority::category level{};
    };

    enum class take_result
    {
        empty,
        skip,
        taken
    };

    void queue_job_handle(const job_handle& handle, std::vector<worker_wakeup>& wakeups)
    {
        job_priority_queues_[handle.group.level].emplace(handle);
        collect_wakeups(handle.group.level, wakeups);
    }

    void collect_wakeups(priority::category job_level, std::vector<worker_wakeup>& wakeups)
    {
        for(const auto& kvp : workers_)
        {
            const auto worker_level = kvp.first;
            if(worker_level > job_level)
            {
                continue;
            }
            auto& pending = pending_wake_[worker_level];
            if(pending)
            {
                continue;
            }
            pending = true;
            for(const auto& worker : kvp.second)
            {
                wakeups.push_back({worker.get_id(), worker_level});
            }
        }
    }

    void dispatch_wakeups(const std::vector<worker_wakeup>& wakeups)
    {
        for(const auto& wakeup : wakeups)
        {
            invoke(wakeup.id,
                   [this, level = wakeup.level]()
                   {
                       check_jobs(level);
                   });
        }
    }

    auto get_highest_priority_queue_above(priority::category level) -> jobs_queue&
    {
        priority::category selected_level = level;
        for(const auto& kvp : job_priority_queues_)
        {
            const auto queue_priority_level = kvp.first;
            if(selected_level <= queue_priority_level && !kvp.second.empty())
            {
                selected_level = queue_priority_level;
            }
        }
        return job_priority_queues_[selected_level];
    }

    auto try_take_job(priority::category level, task& user_job, job_id& id) -> take_result
    {
        std::lock_guard<std::mutex> lock(guard_);
        auto& job_queue = get_highest_priority_queue_above(level);
        if(job_queue.empty())
        {
            pending_wake_[level] = false;
            return take_result::empty;
        }
        const auto handle = job_queue.top();
        job_queue.pop();
        auto it = jobs_.find(handle.id);
        if(it == jobs_.end())
        {
            return take_result::skip;
        }
        auto& job = it->second;
        // change_priority leaves a stale handle in the old queue. The first
        // take moves callable; later handles still find the job but have nothing to run.
        if(level <= job.handle.group.level && job.callable)
        {
            id = job.handle.id;
            user_job = std::move(job.callable);
            return take_result::taken;
        }
        return take_result::skip;
    }

    void check_jobs(priority::category level)
    {
        for(;;)
        {
            if(this_thread::notified_for_exit())
            {
                return;
            }
            task user_job;
            job_id id = 0;
            const auto result = try_take_job(level, user_job, id);
            if(result == take_result::empty)
            {
                return;
            }
            if(result == take_result::skip || !user_job)
            {
                continue;
            }

            if(user_job)
            {
                user_job();
            }
            clear(id, false);
        }
    }

    mutable std::mutex guard_;
    job_id free_id_ = 1;
    priority_workers workers_;
    std::map<priority::category, bool> pending_wake_;
    std::unordered_map<job_id, job_info> jobs_;
    std::unordered_map<job_id, job_info> deferred_jobs_;
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

job_id thread_pool::add_job(task& job, priority::group group, const std::string& name, bool queue)
{
    return impl_->add_job(job, group, name, queue);
}

auto thread_pool::submit(job_id id) -> bool
{
    return impl_->submit(id);
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

void thread_pool::wait_all_polling(priority::category category, const on_progress_callback& on_progress)
{
    impl_->wait_all_polling(category, on_progress);
}

size_t thread_pool::get_jobs_count() const
{
    return impl_->get_jobs_count();
}

size_t thread_pool::get_jobs_count(priority::category category) const
{
    return impl_->get_jobs_count(category);
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

void job_future_storage::submit() const
{
    if(submitted_)
    {
        return;
    }

    if(sentinel_.expired())
    {
        return;
    }

    if(owner_)
    {
        submitted_ = owner_->submit(id);
    }
}

} // namespace tpp
