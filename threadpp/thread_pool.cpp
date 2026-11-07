#include "thread_pool.h"
#include "detail/scheduler.h"

#include <algorithm>
#include <array>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tpp
{
namespace
{
constexpr size_t category_count = 4;

/// The level of the pool worker running on this thread: it takes jobs of that category and above.
thread_local priority::category current_worker_level = priority::category::low;

auto to_index(priority::category level) -> size_t
{
    return static_cast<size_t>(level);
}
} // namespace

class thread_pool::impl final
    : public detail::job_source
    , public std::enable_shared_from_this<impl>
{
    struct job_handle
    {
        job_id id = 0;
        priority::group group;
        /// When it was queued: the first queued starts first among equals. A job queued again
        /// (change_priority) gets a new one, which tells its old handle apart.
        std::uint64_t sequence = 0;
    };

    struct job_info
    {
        job_handle handle;
        std::string name;
        std::string detail;
        /// The job of this pool that scheduled it, 0 when none: a wait inside that job helps with
        /// its own jobs first.
        job_id region = 0;

        task callable;
        shared_future<void> callable_future;
    };

    /// A job from the moment a thread takes it until it returns.
    struct running_entry
    {
        std::string name;
        std::string detail;
        std::chrono::steady_clock::time_point start;
        /// The thread that runs it: a worker, or one that waited for it.
        std::thread::id thread;
    };

    struct idle_worker
    {
        priority::category level{};
        detail::parker_ptr parker;
    };

    /// The threads to wake once the guard is released.
    using wake_list = std::vector<detail::parker_ptr>;

    /// The heap order: lhs starts after rhs.
    static auto starts_after(const job_handle& lhs, const job_handle& rhs) -> bool
    {
        if(lhs.group.level != rhs.group.level)
        {
            return lhs.group.level < rhs.group.level;
        }
        if(lhs.group.priority != rhs.group.priority)
        {
            return lhs.group.priority < rhs.group.priority;
        }
        return lhs.sequence > rhs.sequence;
    }

public:
    impl(const std::map<priority::category, size_t>& workers_per_priority_level, tasks_capacity_config config)
        : workers_per_level_(workers_per_priority_level)
        , config_(config)
        , worker_count_(count_workers(workers_per_priority_level))
    {
        jobs_.reserve(config.default_reserved_tasks);
    }

    impl(impl&&) = delete;
    impl& operator=(impl&&) = delete;
    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;

    ~impl() override
    {
        shutdown();
    }

    /// Starts the workers, or makes the calling thread the host of a pool without any.
    void start()
    {
        if(worker_count_ == 0)
        {
            detail::add_hosted_source(weak_from_this());
            return;
        }
        std::lock_guard<std::mutex> lock(guard_);
        for(const auto& kvp : workers_per_level_)
        {
            const auto level = kvp.first;
            const auto count = kvp.second;
            auto& workers_for_level = workers_[level];
            workers_for_level.reserve(count);
            for(size_t i = 0; i < count; ++i)
            {
                std::string name = "pool worker:" + std::to_string(unsigned(level)) + ":" + std::to_string(i);
                workers_for_level.emplace_back(detail::make_thread_running(name,
                                                                           [this, level]()
                                                                           {
                                                                               run_worker(level);
                                                                           }));
                tpp::set_thread_config(workers_for_level.back().get_id(), config_);
            }
        }
    }

    /// Drops the queued jobs and joins the workers; the running jobs finish first.
    void shutdown()
    {
        std::map<priority::category, std::vector<tpp::thread>> workers;
        {
            std::lock_guard<std::mutex> lock(guard_);
            if(is_shut_down_)
            {
                return;
            }
            is_shut_down_ = true;
            workers = std::move(workers_);
            workers_.clear();
        }
        clear_all();
        workers.clear();
    }

    auto add_job(task& user_job, priority::group group, const job_desc& desc, detail::state_base& user_state, bool queue)
        -> job_id
    {
        auto packaged_task = detail::package_future_task(std::move(user_job));
        const job_id region = detail::current_job_of(this);
        wake_list wakes;
        job_id id = 0;
        {
            std::lock_guard<std::mutex> lock(guard_);
            id = free_id_++;
            auto& target = queue ? jobs_ : deferred_jobs_;
            auto& job = target[id];
            job.handle.id = id;
            job.handle.group = group;
            job.name = desc.name;
            job.detail = desc.detail;
            job.region = region;
            job.callable = std::move(packaged_task.callable);
            job.callable_future = packaged_task.callable_future.share();
            const detail::job_link link{weak_from_this(), id};
            user_state.set_job(link);
            job.callable_future._internal_get_state()->set_job(link);
            if(queue)
            {
                queue_locked(job, wakes);
            }
        }
        wake(wakes);
        return id;
    }

    auto submit(job_id id) -> bool
    {
        wake_list wakes;
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
            queue_locked(job, wakes);
        }
        wake(wakes);
        return true;
    }

    void change_priority(job_id id, priority::group group)
    {
        wake_list wakes;
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
                queue_locked(job, wakes);
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
        wake(wakes);
    }

    void clear(job_id id, bool check_callable)
    {
        // Destroyed once the guard is released: dropping a job breaks its promise, which runs the
        // continuations of its future, and those may use the pool.
        job_info cleared;
        {
            std::lock_guard<std::mutex> lock(guard_);
            auto it = jobs_.find(id);
            if(it != jobs_.end())
            {
                if(!check_callable || it->second.callable)
                {
                    cleared = std::move(it->second);
                    jobs_.erase(it);
                }
                return;
            }

            auto dit = deferred_jobs_.find(id);
            if(dit != deferred_jobs_.end())
            {
                if(!check_callable || dit->second.callable)
                {
                    cleared = std::move(dit->second);
                    deferred_jobs_.erase(dit);
                }
            }
        }
    }

    void clear_all()
    {
        std::unordered_map<job_id, job_info> jobs;
        std::unordered_map<job_id, job_info> deferred_jobs;
        {
            std::lock_guard<std::mutex> lock(guard_);
            jobs = std::move(jobs_);
            jobs_.clear();
            deferred_jobs = std::move(deferred_jobs_);
            deferred_jobs_.clear();
            queue_.clear();
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

    /// A job a wait_all with progress is for.
    struct waited_job
    {
        std::string name;
        std::string detail;
        shared_future<void> future;
    };

    auto collect_waited_jobs(priority::category category) const -> std::vector<waited_job>
    {
        std::vector<waited_job> futures;
        std::lock_guard<std::mutex> lock(guard_);
        futures.reserve(jobs_.size());
        for(const auto& kvp : jobs_)
        {
            const auto& job = kvp.second;
            if(job.handle.group.level >= category)
            {
                futures.push_back({job.name, job.detail, job.callable_future});
            }
        }
        return futures;
    }

    void wait_all(priority::category category, const on_progress_callback& on_progress)
    {
        const std::vector<waited_job> futures = collect_waited_jobs(category);
        size_t current_job = 0;
        for(const auto& waited : futures)
        {
            waited.future.wait();
            current_job++;
            if(on_progress)
            {
                progress_info info;
                info.name = waited.name;
                info.detail = waited.detail;
                info.current_job = current_job;
                info.total_jobs = futures.size();
                on_progress(info);
            }
        }
    }

    void wait_all_polling(priority::category category, const on_progress_callback& on_progress)
    {
        const std::vector<waited_job> futures = collect_waited_jobs(category);
        // Counted on every report, not taken from the position of the waited job: the jobs
        // finish in any order, and the one waited on may be the last to.
        const auto count_finished = [&futures]() -> size_t
        {
            return static_cast<size_t>(std::count_if(futures.begin(),
                                                     futures.end(),
                                                     [](const waited_job& waited)
                                                     {
                                                         return waited.future.is_ready();
                                                     }));
        };
        for(const auto& waited : futures)
        {
            while(!waited.future.is_ready())
            {
                waited.future.wait_for(std::chrono::milliseconds(16));
                if(on_progress)
                {
                    progress_info info;
                    info.name = waited.name;
                    info.detail = waited.detail;
                    info.current_job = count_finished();
                    info.total_jobs = futures.size();
                    on_progress(info);
                }
            }
        }
    }

    auto run_pending(std::chrono::steady_clock::duration budget) -> size_t
    {
        const auto start = std::chrono::steady_clock::now();
        size_t run_count = 0;
        for(;;)
        {
            task user_job;
            job_id id = 0;
            {
                std::lock_guard<std::mutex> lock(guard_);
                if(!take_next_locked(priority::category::low, user_job, id))
                {
                    return run_count;
                }
            }
            run_taken(user_job, id);
            ++run_count;
            if(std::chrono::steady_clock::now() - start >= budget)
            {
                return run_count;
            }
        }
    }

    auto get_running_jobs() const -> std::vector<running_job>
    {
        std::vector<running_job> result;
        {
            std::lock_guard<std::mutex> lock(guard_);
            const auto now = std::chrono::steady_clock::now();
            result.reserve(running_.size());
            for(const auto& kvp : running_)
            {
                const running_entry& entry = kvp.second;
                result.push_back({entry.name, entry.detail, now - entry.start});
            }
        }
        std::sort(result.begin(),
                  result.end(),
                  [](const running_job& lhs, const running_job& rhs)
                  {
                      return lhs.elapsed > rhs.elapsed;
                  });
        return result;
    }

    auto get_workers_count() const -> size_t
    {
        return worker_count_;
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
            result[kvp.second.name]++;
        }
        return result;
    }

    //-----------------------------------------------------------------------------
    /// detail::job_source
    //-----------------------------------------------------------------------------
    auto run_waited_job(std::uint64_t id, bool is_timed) -> bool override
    {
        task user_job;
        std::string report;
        {
            std::lock_guard<std::mutex> lock(guard_);
            auto it = jobs_.find(id);
            if(it == jobs_.end())
            {
                return false;
            }
            auto& job = it->second;
            if(!job.callable)
            {
                report = report_wait_on_own_stack_locked(id);
            }
            else if(may_run_waited_job_locked(job, is_timed))
            {
                take_locked(job, user_job);
            }
        }
        if(!report.empty())
        {
            detail::log_error(report);
        }
        if(!user_job)
        {
            return false;
        }
        run_taken(user_job, id);
        return true;
    }

    auto run_job_to_help(bool /*is_timed*/) -> bool override
    {
        const bool is_host = worker_count_ == 0 && detail::is_host_of(this);
        const bool is_worker = worker_count_ != 0 && detail::is_worker_of(this);
        if(!is_host && !is_worker)
        {
            return false;
        }
        const job_id region = detail::current_job_of(this);
        const auto min_level = is_worker ? current_worker_level : priority::category::low;
        task user_job;
        job_id id = 0;
        std::string note;
        {
            std::lock_guard<std::mutex> lock(guard_);
            // A worker helps only when every worker is busy: a free one takes the job otherwise.
            if(is_worker && has_available_worker_locked(priority::category::critical))
            {
                return false;
            }
            if(!take_best_locked(min_level, &region, user_job, id))
            {
                // Nothing of its own region is queued: any queued job, as nothing else may run the one
                // the wait is for. A host says so, as that job may then wait for the wait to end.
                if(!take_best_locked(min_level, nullptr, user_job, id))
                {
                    return false;
                }
                if(is_host)
                {
                    note = note_widened_locked(region, id);
                }
            }
        }
        if(!note.empty())
        {
            detail::log_info(note);
        }
        run_taken(user_job, id);
        return true;
    }

    auto can_help() const -> bool override
    {
        return (worker_count_ == 0 && detail::is_host_of(this)) || detail::is_worker_of(this);
    }

    void add_parked_helper(const detail::parker_ptr& helper) override
    {
        std::lock_guard<std::mutex> lock(guard_);
        parked_helpers_.push_back(helper);
    }

    void remove_parked_helper(const detail::parker_ptr& helper) override
    {
        std::lock_guard<std::mutex> lock(guard_);
        auto it = std::find(parked_helpers_.begin(), parked_helpers_.end(), helper);
        if(it != parked_helpers_.end())
        {
            parked_helpers_.erase(it);
        }
    }

    auto describe_job(std::uint64_t id) const -> std::string override
    {
        std::lock_guard<std::mutex> lock(guard_);
        return describe_job_locked(id);
    }

private:
    static auto count_workers(const std::map<priority::category, size_t>& workers_per_priority_level) -> size_t
    {
        size_t count = 0;
        for(const auto& kvp : workers_per_priority_level)
        {
            count += kvp.second;
        }
        return count;
    }

    static void wake(const wake_list& wakes)
    {
        for(const auto& parker : wakes)
        {
            parker->unpark();
        }
    }

    /// A worker's life: its thread's tasks, then the pool's jobs, then sleep until either comes.
    void run_worker(priority::category level)
    {
        detail::worker_scope scope(weak_from_this());
        current_worker_level = level;
        const auto parker = detail::current_parker();
        // Woken for a queued job: it counts as free until it has looked for one.
        bool is_claimed = false;
        for(;;)
        {
            // The ticket before the exit check: an exit notified in between still ends the park below.
            const auto ticket = parker->prepare();
            if(this_thread::notified_for_exit())
            {
                break;
            }
            this_thread::process();
            const bool has_run = run_next_job(level, is_claimed);
            is_claimed = false;
            if(has_run || !enter_idle(level, parker))
            {
                continue;
            }
            parker->park(ticket);
            is_claimed = leave_idle(parker);
        }
        std::lock_guard<std::mutex> lock(guard_);
        release_idle_locked(level, parker, is_claimed);
    }

    auto run_next_job(priority::category level, bool is_claimed) -> bool
    {
        task user_job;
        job_id id = 0;
        wake_list wakes;
        {
            std::lock_guard<std::mutex> lock(guard_);
            release_idle_locked(level, nullptr, is_claimed);
            if(!take_next_locked(level, user_job, id))
            {
                return false;
            }
            // This worker is busy now: a thread that left the job it waits for to a free worker
            // may have to run it itself.
            if(!parked_helpers_.empty() && has_queued_job_locked(priority::category::low) &&
               !has_available_worker_locked(queue_.front().group.level))
            {
                wakes = parked_helpers_;
            }
        }
        wake(wakes);
        run_taken(user_job, id);
        return true;
    }

    /// Records the worker as idle unless a job it may take is queued; false then.
    auto enter_idle(priority::category level, const detail::parker_ptr& parker) -> bool
    {
        std::lock_guard<std::mutex> lock(guard_);
        if(has_queued_job_locked(level))
        {
            return false;
        }
        idle_workers_.push_back({level, parker});
        return true;
    }

    /// Takes the worker off the idle list; true when a queued job took it off already (claimed it).
    auto leave_idle(const detail::parker_ptr& parker) -> bool
    {
        std::lock_guard<std::mutex> lock(guard_);
        return !remove_idle_locked(parker);
    }

    auto remove_idle_locked(const detail::parker_ptr& parker) -> bool
    {
        auto it = std::find_if(idle_workers_.begin(),
                               idle_workers_.end(),
                               [&parker](const idle_worker& idle)
                               {
                                   return idle.parker == parker;
                               });
        if(it == idle_workers_.end())
        {
            return false;
        }
        idle_workers_.erase(it);
        return true;
    }

    /// The worker is neither idle nor claimed any more.
    void release_idle_locked(priority::category level, const detail::parker_ptr& parker, bool is_claimed)
    {
        if(parker)
        {
            remove_idle_locked(parker);
        }
        auto& waking = waking_[to_index(level)];
        if(is_claimed && waking > 0)
        {
            --waking;
        }
    }

    /// N = 0: any waiter. Otherwise only when no free worker would take it, and never in a timed wait of a
    /// thread outside the pool, which must return near its deadline.
    auto may_run_waited_job_locked(const job_info& job, bool is_timed) const -> bool
    {
        if(worker_count_ == 0)
        {
            return true;
        }
        if(is_timed && !detail::is_worker_of(this))
        {
            return false;
        }
        return !has_available_worker_locked(job.handle.group.level);
    }

    /// Whether a free worker could take a job of the level: one idle, or one woken for a job that has not
    /// taken one yet.
    auto has_available_worker_locked(priority::category level) const -> bool
    {
        const bool has_idle = std::any_of(idle_workers_.begin(),
                                          idle_workers_.end(),
                                          [level](const idle_worker& idle)
                                          {
                                              return idle.level <= level;
                                          });
        if(has_idle)
        {
            return true;
        }
        for(size_t i = 0; i <= to_index(level) && i < category_count; ++i)
        {
            if(waking_[i] > 0)
            {
                return true;
            }
        }
        return false;
    }

    /// Wakes the idle worker of the highest level that may take a job of `level`. False when none is idle.
    auto claim_idle_worker_locked(priority::category level, wake_list& wakes) -> bool
    {
        auto best = idle_workers_.end();
        for(auto it = idle_workers_.begin(); it != idle_workers_.end(); ++it)
        {
            if(it->level <= level && (best == idle_workers_.end() || best->level < it->level))
            {
                best = it;
            }
        }
        if(best == idle_workers_.end())
        {
            return false;
        }
        ++waking_[to_index(best->level)];
        wakes.push_back(best->parker);
        idle_workers_.erase(best);
        return true;
    }

    void queue_locked(job_info& job, wake_list& wakes)
    {
        job.handle.sequence = free_sequence_++;
        queue_.push_back(job.handle);
        std::push_heap(queue_.begin(), queue_.end(), starts_after);
        if(!claim_idle_worker_locked(job.handle.group.level, wakes))
        {
            // No worker takes it soon: the threads waiting that may run it look again.
            wakes.insert(wakes.end(), parked_helpers_.begin(), parked_helpers_.end());
        }
    }

    static auto is_current(const job_handle& handle, const job_info& job) -> bool
    {
        return job.callable && job.handle.sequence == handle.sequence;
    }

    /// Drops the handles at the top of the queue whose jobs were taken, stopped or queued again.
    void drop_stale_top_locked()
    {
        while(!queue_.empty())
        {
            auto it = jobs_.find(queue_.front().id);
            if(it != jobs_.end() && is_current(queue_.front(), it->second))
            {
                return;
            }
            std::pop_heap(queue_.begin(), queue_.end(), starts_after);
            queue_.pop_back();
        }
    }

    auto has_queued_job_locked(priority::category level) -> bool
    {
        drop_stale_top_locked();
        return !queue_.empty() && queue_.front().group.level >= level;
    }

    /// Takes the job that starts next, if it is of the level or above.
    auto take_next_locked(priority::category level, task& user_job, job_id& id) -> bool
    {
        if(!has_queued_job_locked(level))
        {
            return false;
        }
        const auto handle = queue_.front();
        std::pop_heap(queue_.begin(), queue_.end(), starts_after);
        queue_.pop_back();
        id = handle.id;
        take_locked(jobs_.at(id), user_job);
        return true;
    }

    /// Takes the job that starts first among those of the level or above and, given one, of the region.
    /// Its handle stays queued, stale; whoever meets it drops it.
    auto take_best_locked(priority::category level, const job_id* region, task& user_job, job_id& id) -> bool
    {
        const job_handle* best = nullptr;
        job_info* best_job = nullptr;
        for(const auto& handle : queue_)
        {
            if(handle.group.level < level || (best != nullptr && !starts_after(*best, handle)))
            {
                continue;
            }
            auto it = jobs_.find(handle.id);
            if(it == jobs_.end() || !is_current(handle, it->second) ||
               (region != nullptr && it->second.region != *region))
            {
                continue;
            }
            best = &handle;
            best_job = &it->second;
        }
        if(best_job == nullptr)
        {
            return false;
        }
        id = best->id;
        take_locked(*best_job, user_job);
        drop_stale_top_locked();
        return true;
    }

    /// Moves the job's callable out to run on the caller and records the job as running.
    void take_locked(job_info& job, task& user_job)
    {
        user_job = std::move(job.callable);
        running_[job.handle.id] = {job.name, job.detail, std::chrono::steady_clock::now(), std::this_thread::get_id()};
    }

    void run_taken(task& user_job, job_id id)
    {
        {
            detail::job_scope scope(this, id);
            user_job();
            user_job = {};
        }
        finish_job(id);
    }

    void finish_job(job_id id)
    {
        job_info finished;
        {
            std::lock_guard<std::mutex> lock(guard_);
            running_.erase(id);
            reported_own_stack_waits_.erase(id);
            auto it = jobs_.find(id);
            if(it != jobs_.end())
            {
                finished = std::move(it->second);
                jobs_.erase(it);
            }
        }
    }

    auto describe_job_locked(job_id id) const -> std::string
    {
        const auto describe = [](const std::string& name, const std::string& detail)
        {
            std::string text = "job '" + name + "'";
            if(!detail.empty())
            {
                text += " (" + detail + ")";
            }
            return text;
        };
        auto it = jobs_.find(id);
        if(it != jobs_.end())
        {
            return describe(it->second.name, it->second.detail);
        }
        auto dit = deferred_jobs_.find(id);
        if(dit != deferred_jobs_.end())
        {
            return describe(dit->second.name, dit->second.detail);
        }
        auto rit = running_.find(id);
        if(rit != running_.end())
        {
            return describe(rit->second.name, rit->second.detail);
        }
        return "job " + std::to_string(id);
    }

    /// A job this thread runs further up its stack cannot end while the thread waits for it, so the wait
    /// never returns. Reported once per job.
    auto report_wait_on_own_stack_locked(job_id id) -> std::string
    {
        if(!detail::is_running_here(this, id) || !reported_own_stack_waits_.insert(id).second)
        {
            return {};
        }
        return "[tpp] A wait for " + describe_job_locked(id) +
               ", which runs further up the same thread, never returns.";
    }

    /// A host that found nothing of the region to run ran another job: said once per region job name.
    auto note_widened_locked(job_id region, job_id id) -> std::string
    {
        const std::string waiting = region == 0 ? "outside any job" : "inside " + describe_job_locked(region);
        if(!reported_widened_.insert(waiting).second)
        {
            return {};
        }
        return "[tpp] A wait " + waiting + " has none of its own jobs queued and runs other queued jobs, " +
               describe_job_locked(id) + " first, as the pool has no workers.";
    }

    const std::map<priority::category, size_t> workers_per_level_;
    const tasks_capacity_config config_;
    /// Set once: the pool never changes its workers.
    const size_t worker_count_;

    mutable std::mutex guard_;
    job_id free_id_ = 1;
    std::uint64_t free_sequence_ = 1;
    std::map<priority::category, std::vector<tpp::thread>> workers_;
    /// The queued jobs' handles, a heap in starts_after order; stale handles stay until met.
    std::vector<job_handle> queue_;
    std::unordered_map<job_id, job_info> jobs_;
    std::unordered_map<job_id, job_info> deferred_jobs_;
    /// Outlives a clear_all: the jobs in it are still running.
    std::unordered_map<job_id, running_entry> running_;
    std::vector<idle_worker> idle_workers_;
    /// Workers a queued job woke that have not looked for a job yet, per level.
    std::array<size_t, category_count> waking_{};
    /// Threads blocked in waits that may run this pool's jobs.
    std::vector<detail::parker_ptr> parked_helpers_;
    std::unordered_set<job_id> reported_own_stack_waits_;
    std::set<std::string> reported_widened_;
    bool is_shut_down_ = false;
};

////////////////////////////////////////////////////////////
thread_pool::thread_pool() : thread_pool({{priority::category::low, thread::hardware_concurrency()}})
{
}
thread_pool::thread_pool(const std::map<priority::category, size_t>& workers_per_priority_level,
                         tasks_capacity_config config)
    : impl_(std::make_shared<impl>(workers_per_priority_level, config))
{
    impl_->start();
}

thread_pool::thread_pool(thread_pool&& other) noexcept = default;

auto thread_pool::operator=(thread_pool&& other) noexcept -> thread_pool&
{
    if(this != &other)
    {
        if(impl_)
        {
            impl_->shutdown();
        }
        impl_ = std::move(other.impl_);
    }
    return *this;
}

thread_pool::~thread_pool()
{
    // The workers are joined here, not when the last wait lets go of the core.
    if(impl_)
    {
        impl_->shutdown();
    }
}

auto thread_pool::add_job(task& job,
                          priority::group group,
                          const job_desc& desc,
                          detail::state_base& user_state,
                          bool queue) -> job_id
{
    return impl_->add_job(job, group, desc, user_state, queue);
}

auto thread_pool::get_job_source() const -> std::weak_ptr<detail::job_source>
{
    return impl_;
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

auto thread_pool::run_pending(std::chrono::steady_clock::duration budget) -> size_t
{
    return impl_->run_pending(budget);
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

auto thread_pool::get_running_jobs() const -> std::vector<running_job>
{
    return impl_->get_running_jobs();
}

auto thread_pool::get_workers_count() const -> size_t
{
    return impl_->get_workers_count();
}

void job_future_storage::change_priority(priority::group group)
{
    if(auto source = source_.lock())
    {
        std::static_pointer_cast<thread_pool::impl>(source)->change_priority(id, group);
    }
}

void job_future_storage::stop()
{
    if(auto source = source_.lock())
    {
        std::static_pointer_cast<thread_pool::impl>(source)->clear(id, true);
    }
}

void job_future_storage::submit() const
{
    if(is_submitted())
    {
        return;
    }
    auto source = source_.lock();
    if(!source)
    {
        return;
    }

    // One caller submits; the others see the job taken. The flag still ends up as the pool's
    // answer: false when the pool no longer holds the job as deferred.
    if(submitted_->exchange(true, std::memory_order_acq_rel))
    {
        return;
    }
    if(!std::static_pointer_cast<thread_pool::impl>(source)->submit(id))
    {
        submitted_->store(false, std::memory_order_release);
    }
}

} // namespace tpp
