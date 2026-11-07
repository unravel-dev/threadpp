#pragma once

#include "future.hpp"
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>
namespace tpp
{

namespace priority
{

enum class category : size_t
{
    low,
    normal,
    high,
    critical
};

struct group
{
    group() = default;
    group(category c, size_t pr) : level(c), priority(pr)
    {
    }

    category level = category::normal;
    size_t priority = 0;
};

inline auto operator==(const group& lhs, const group& rhs) -> bool
{
    return lhs.level == rhs.level && lhs.priority == rhs.priority;
}

inline auto low(size_t priority = 0) -> group
{
    return {category::low, priority};
}
inline auto normal(size_t priority = 0) -> group
{
    return {category::normal, priority};
}
inline auto high(size_t priority = 0) -> group
{
    return {category::high, priority};
}
inline auto critical(size_t priority = 0) -> group
{
    return {category::critical, priority};
}

} // namespace priority

using job_id = uint64_t;
class thread_pool;

struct job_future_storage
{
    friend class thread_pool;
    job_id id = 0;

    job_future_storage() = default;

    //-----------------------------------------------------------------------------
    /// Changes the priority level of the specified job.
    /// Increasing the priority will cause the job to be executed sooner.
    //-----------------------------------------------------------------------------
    void change_priority(priority::group group);

    //-----------------------------------------------------------------------------
    /// Attempt to stop a job by its id. If the job is already executing
    /// this function will do nothing.
    //-----------------------------------------------------------------------------
    void stop();

    //-----------------------------------------------------------------------------
    /// Submits a deferred job to the work queue so workers can pick it up.
    /// Does nothing if the job was already submitted or the pool is gone.
    /// Safe to call on one future from several threads at once: one of them submits.
    //-----------------------------------------------------------------------------
    void submit() const;

    //-----------------------------------------------------------------------------
    /// Returns true if the job has been submitted to the work queue.
    /// Deferred jobs return false until submit() is called.
    //-----------------------------------------------------------------------------
    auto is_submitted() const -> bool
    {
        return submitted_ == nullptr || submitted_->load(std::memory_order_acquire);
    }

private:
    /// The pool's core, which outlives the pool while a wait uses it; expired once the pool is gone.
    std::weak_ptr<detail::job_source> source_{};
    /// Whether the pool took a deferred job; null for a job scheduled at once. One flag for every copy
    /// of the future, so a copy taken before another one submitted sees the submission. Atomic
    /// because submit() is const and copies are read, and submitted, from any thread.
    std::shared_ptr<std::atomic<bool>> submitted_{};
};

template<typename T>
struct job_shared_future;

template<>
struct job_shared_future<void>;
// Just a normal future with
// a job_id member
template<typename T>
struct job_future
    : future<T>
    , job_future_storage
{
    job_future(future<T>&& rhs) noexcept : future<T>(std::move(rhs))
    {
    }
    job_future() = default;
    job_future(job_future&& rhs) noexcept = default;
    job_future(const job_future&) = delete;
    auto operator=(job_future&& rhs) noexcept -> job_future& = default;
    auto operator=(const job_future&) -> job_future& = delete;

    auto share() -> job_shared_future<T>
    {
        return job_shared_future<T>(std::move(*this));
    }

    auto use_count() const noexcept -> decltype(auto)
    {
        return this->state_.use_count();
    }

    auto get() -> T
    {
        this->submit();
        return this->future<T>::get();
    }
};


template<typename T>
struct job_shared_future
    : shared_future<T>
    , job_future_storage
{
    job_shared_future(job_future<T>&& uf) noexcept
        : shared_future<T>(static_cast<future<T>&&>(uf))
        , job_future_storage(static_cast<job_future_storage&&>(uf))
    {
    }
    job_shared_future() noexcept = default;
    job_shared_future(const job_shared_future& sf) = default;
    job_shared_future(job_shared_future&& sf) noexcept = default;

    auto operator=(const job_shared_future& sf) -> job_shared_future& = default;
    auto operator=(job_shared_future&& sf) noexcept -> job_shared_future& = default;

    auto use_count() const noexcept -> decltype(auto)
    {
        return this->state_.use_count();
    }

    auto get() const -> const T&
    {
        this->submit();
        return this->shared_future<T>::get();
    }
};

template<>
struct job_shared_future<void>
    : shared_future<void>
    , job_future_storage
{
    job_shared_future(job_future<void>&& uf) noexcept
        : shared_future<void>(static_cast<future<void>&&>(uf))
        , job_future_storage(static_cast<job_future_storage&&>(uf))
    {
    }
    job_shared_future() noexcept = default;
    job_shared_future(const job_shared_future& sf) = default;
    job_shared_future(job_shared_future&& sf) noexcept = default;

    auto operator=(const job_shared_future& sf) -> job_shared_future& = default;
    auto operator=(job_shared_future&& sf) noexcept -> job_shared_future& = default;

    auto use_count() const noexcept -> decltype(auto)
    {
        return this->state_.use_count();
    }

    void get() const
    {
        this->submit();
        this->shared_future<void>::get();
    }
};


template<typename F, typename... Args>
using job_ret_type = callable_ret_type<F, Args...>;

//-----------------------------------------------------------------------------
/// What a job is, for whoever watches the pool. The name says what kind of
/// work it is and groups jobs of a kind (get_jobs_count_detailed counts by it);
/// the detail tells one job of that kind from another, e.g. the file it works on.
//-----------------------------------------------------------------------------
struct job_desc
{
    std::string name{};
    std::string detail{};
};

//-----------------------------------------------------------------------------
/// A job a worker is running, as get_running_jobs saw it.
//-----------------------------------------------------------------------------
struct running_job
{
    std::string name{};
    std::string detail{};
    /// How long it had been running when the snapshot was taken.
    std::chrono::steady_clock::duration elapsed{};
};

//-----------------------------------------------------------------------------
/// Thread pool class. Can have multiple priority groups.
//-----------------------------------------------------------------------------
class thread_pool
{
public:
    //-----------------------------------------------------------------------------
    /// Creates a thread_pool with specified workers per priority level.
    /// E.g
    /// tpp::thread_pool pool({{tpp::priority::category::normal, 2},
    ///					       {tpp::priority::category::high, 1},
    ///					       {tpp::priority::category::critical, 1}});
    ///
    /// Jobs start highest category first, then highest priority, then first
    /// scheduled first. A thread that waits for a job's future, or for a future made
    /// from one (then, when_all, when_any), runs the job itself when no worker is
    /// free to: always in a pool without workers, and in an untimed wait when every
    /// worker is busy. A timed wait runs no job while the pool has workers.
    ///
    /// A pool with no workers at all, for a program that has no threads to give it,
    /// is hosted by the thread that creates it: its waits and sleeps also run queued
    /// jobs when nothing they depend on is queued, and run_pending(), which such a
    /// program calls regularly, runs the jobs nobody waits for.
    //-----------------------------------------------------------------------------
    thread_pool(const std::map<priority::category, size_t>& workers_per_priority_level,
                tasks_capacity_config config = {});
    thread_pool();
    thread_pool(thread_pool&& other) noexcept;
    auto operator=(thread_pool&& other) noexcept -> thread_pool&;
    ~thread_pool();

    thread_pool(const thread_pool&) = delete;
    auto operator=(const thread_pool&) -> thread_pool& = delete;

    //-----------------------------------------------------------------------------
    /// Adds a job for a certain priority level.
    /// Returns a future to the job.
    //-----------------------------------------------------------------------------
    template<typename F, typename... Args>
    auto schedule(const job_desc& desc, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    template<typename F, typename... Args>
    auto schedule(const std::string& name, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    template<typename F, typename... Args>
    auto schedule(priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    //-----------------------------------------------------------------------------
    /// Adds a job with default priority level.
    /// Returns a future to the job.
    //-----------------------------------------------------------------------------
    template<typename F, typename... Args>
    auto schedule(F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;


    template<typename F, typename... Args>
    auto schedule(const job_desc& desc, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    template<typename F, typename... Args>
    auto schedule(const std::string& name, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    //-----------------------------------------------------------------------------
    /// Creates a job -- packages the task and creates a valid future
    /// but does NOT queue it to workers. The callable is held on the future
    /// itself. Call submit() on the returned future to actually start execution.
    //-----------------------------------------------------------------------------
    template<typename F, typename... Args>
    auto create_job(const job_desc& desc, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    template<typename F, typename... Args>
    auto create_job(const std::string& name, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    template<typename F, typename... Args>
    auto create_job(priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    template<typename F, typename... Args>
    auto create_job(F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    template<typename F, typename... Args>
    auto create_job(const std::string& name, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

    //-----------------------------------------------------------------------------
    /// Submits a previously created deferred job to the work queue.
    /// Returns true if the job was found and queued, false otherwise.
    //-----------------------------------------------------------------------------
    auto submit(job_id id) -> bool;

    //-----------------------------------------------------------------------------
    /// Changes the priority level of the specified job.
    /// Increasing the priority will cause the job to be executed sooner.
    //-----------------------------------------------------------------------------
    void change_priority(job_id id, priority::group group);

    //-----------------------------------------------------------------------------
    /// Attempt to stop a job by its id. If the job is already executing
    /// this function will do nothing.
    //-----------------------------------------------------------------------------
    void stop(job_id id);

    //-----------------------------------------------------------------------------
    /// Stop all pending jobs. This call will not stop any jobs that
    /// are currently running.
    //-----------------------------------------------------------------------------
    void stop_all();

    //-----------------------------------------------------------------------------
    /// Blocks until the job matching the passed id is ready.
    /// It is recommended that you use the future returned by 'schedule'
    /// function as it gives richer options and can also retrieve a return value,
    /// query the readiness and etc.
    //-----------------------------------------------------------------------------
    void wait(job_id id);

    //-----------------------------------------------------------------------------
    /// Blocks until all active jobs are ready.
    //-----------------------------------------------------------------------------
    void wait_all();
    struct progress_info
    {
        /// The job the wait is on, one that has not finished yet.
        std::string name{};
        std::string detail{};
        /// How many of the jobs the wait is for have finished.
        size_t current_job{};
        size_t total_jobs{};
    };
    using on_progress_callback = std::function<void(const progress_info& info)>;
    void wait_all(priority::category category, const on_progress_callback& on_progress = nullptr);
    //-----------------------------------------------------------------------------
    /// Waits for the jobs of the category and above that are in the pool when
    /// it is called, and reports every few milliseconds while it waits.
    //-----------------------------------------------------------------------------
    void wait_all_polling(priority::category category, const on_progress_callback& on_progress = nullptr);

    //-----------------------------------------------------------------------------
    /// Runs queued jobs on the calling thread, the highest priority first, until none
    /// is left or the budget is spent; at least one when any is queued. Returns how
    /// many it ran. Deferred jobs that were never submitted are not run.
    //-----------------------------------------------------------------------------
    auto run_pending(std::chrono::steady_clock::duration budget) -> size_t;

    //-----------------------------------------------------------------------------
    /// Returns the number of jobs left, the running ones included.
    //-----------------------------------------------------------------------------
    auto get_jobs_count() const -> size_t;
    auto get_jobs_count_detailed() const -> std::map<std::string, size_t>;
    auto get_jobs_count(priority::category category) const -> size_t;

    //-----------------------------------------------------------------------------
    /// The jobs the workers are running right now, the longest running first.
    //-----------------------------------------------------------------------------
    auto get_running_jobs() const -> std::vector<running_job>;

    //-----------------------------------------------------------------------------
    /// Returns the number of workers of all priority levels.
    //-----------------------------------------------------------------------------
    auto get_workers_count() const -> size_t;

private:
    friend struct job_future_storage;

    /// Adds the job and links the user's future to it, so a wait on that future can run it.
    auto add_job(task& job, priority::group group, const job_desc& desc, detail::state_base& user_state, bool queue)
        -> job_id;

    template<typename T>
    void attach_job(job_future<T>& fut, task& job, priority::group group, const job_desc& desc, bool queue);

    auto get_job_source() const -> std::weak_ptr<detail::job_source>;

    class impl;
    /// pimpl idiom; shared with the waits that run its jobs, which hold it weakly
    std::shared_ptr<impl> impl_;
};

template<typename T>
void thread_pool::attach_job(job_future<T>& fut, task& job, priority::group group, const job_desc& desc, bool queue)
{
    if(!queue)
    {
        fut.submitted_ = std::make_shared<std::atomic<bool>>(false);
    }
    fut.source_ = get_job_source();
    fut.id = add_job(job, group, desc, *fut._internal_get_state(), queue);
}

template<typename F, typename... Args>
auto thread_pool::schedule(const job_desc& desc, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    auto packaged_task = detail::package_future_task(std::forward<F>(f), std::forward<Args>(args)...);
    job_future<async_ret_type<F, Args...>> fut(std::move(packaged_task.callable_future));
    attach_job(fut, packaged_task.callable, group, desc, true);
    return fut;
}

template<typename F, typename... Args>
auto thread_pool::schedule(const std::string& name, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return schedule(job_desc{name, {}}, group, std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::schedule(priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return schedule(job_desc{}, group, std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::schedule(F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return schedule(job_desc{}, priority::normal(), std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::schedule(const job_desc& desc, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return schedule(desc, priority::normal(), std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::schedule(const std::string& name, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return schedule(job_desc{name, {}}, priority::normal(), std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::create_job(const job_desc& desc, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    auto packaged_task = detail::package_future_task(std::forward<F>(f), std::forward<Args>(args)...);
    job_future<async_ret_type<F, Args...>> fut(std::move(packaged_task.callable_future));
    attach_job(fut, packaged_task.callable, group, desc, false);
    return fut;
}

template<typename F, typename... Args>
auto thread_pool::create_job(const std::string& name, priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return create_job(job_desc{name, {}}, group, std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::create_job(priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return create_job(job_desc{}, group, std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::create_job(F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return create_job(job_desc{}, priority::normal(), std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto thread_pool::create_job(const std::string& name, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
    return create_job(job_desc{name, {}}, priority::normal(), std::forward<F>(f), std::forward<Args>(args)...);
}

} // namespace tpp
