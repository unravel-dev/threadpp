#include "thread.h"
#include "detail/parker.h"
#include "detail/scheduler.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
namespace tpp
{

struct thread_context
{
    std::atomic<thread::id> id{invalid_id()};
    std::atomic<std::thread::id> native_thread_id;
    std::mutex tasks_mutex;
    std::vector<task> tasks;

    std::vector<task> processing_tasks;
    std::size_t processing_idx{0};
    std::size_t capacity_shrink_threashold{0};

    // invoke_on_process tasks: queued here, moved to on_process_batch by the
    // first process() call after the previous batch is done
    std::vector<task> on_process_tasks;
    std::vector<task> on_process_batch;
    std::size_t on_process_idx{0};
    // processing_stack_depth of the on-process task running, 0 when none
    std::uint32_t on_process_depth{0};

    // what the thread sleeps on: queued tasks, notify, exit and whatever it waits for unpark it
    detail::parker_ptr parker{std::make_shared<detail::parker>()};
    std::atomic<std::uint32_t> processing_stack_depth{0};

    std::string name;
    bool external{false};
    std::atomic<bool> exit{false};
};

struct program_context
{
    std::atomic<thread::id> id_generator{};
    std::condition_variable cleanup_event;
    std::mutex mutex;
    std::unordered_map<std::thread::id, thread::id> id_map;
    std::unordered_map<thread::id, std::shared_ptr<thread_context>> contexts;
    thread::id main_thread_id{invalid_id()};
    std::atomic<size_t> init_count{0};
    init_data config;

    // what runs async and then without a thread id; empty: a thread each
    std::mutex detached_mutex;
    detached_executor detached;

    auto get_remaining_owned_threads() -> int
    {
        int remaining = 0;
        for(const auto& context : contexts)
        {
            if(!context.second->external)
            {
                remaining++;
            }
        }
        return remaining;
    }

};

#define log_info_func(msg)  log_info("[tpp::" + std::string(__func__) + "] : " + (msg))
#define log_error_func(msg) log_error("[tpp::" + std::string(__func__) + "] : " + (msg))
namespace
{
program_context global_data;
thread_local thread_context* local_data = nullptr;
} // namespace

auto get_global_context() -> program_context&
{
    return global_data;
}
void set_local_context(thread_context* context)
{
    local_data = context;
}
auto has_local_context() -> bool
{
    return !(local_data == nullptr);
}
auto get_local_context() -> thread_context&
{
    return *local_data;
}

void name_thread(const std::string& name)
{
    const auto& global_context = get_global_context();
    if(global_context.config.set_thread_name && !name.empty())
    {
        global_context.config.set_thread_name(name);
    }
}

void on_thread_start(const std::string& name)
{
    const auto& global_context = get_global_context();
    if(global_context.config.on_thread_start && !name.empty())
    {
        global_context.config.on_thread_start(name);
    }
}

void log_info(const std::string& name)
{
    const auto& global_context = get_global_context();
    if(global_context.config.log_info)
    {
        global_context.config.log_info(name);
    }
}

void log_error(const std::string& name)
{
    const auto& global_context = get_global_context();
    if(global_context.config.log_error)
    {
        global_context.config.log_error(name);
    }
}

auto register_thread_impl(std::thread::id native_thread_id, const std::string& name, bool external = false) -> std::shared_ptr<thread_context>
{
    auto& global_context = get_global_context();
    std::unique_lock<std::mutex> lock(global_context.mutex);
    auto id = [&]()
    {
        auto tidit = global_context.id_map.find(native_thread_id);
        if(tidit != global_context.id_map.end())
        {
            return tidit->second;
        }
        return invalid_id();
    }();

    if(id == invalid_id())
    {
        id = ++global_context.id_generator;
    }

    auto it = global_context.contexts.find(id);
    if(it != global_context.contexts.end())
    {
        return it->second;
    }

    auto local_context = std::make_shared<thread_context>();
    local_context->tasks.reserve(global_context.config.tasks_capacity.default_reserved_tasks);
    local_context->native_thread_id = native_thread_id;
    local_context->id = id;
    local_context->capacity_shrink_threashold = global_context.config.tasks_capacity.capacity_shrink_threashold;
    local_context->external = external;
    local_context->name = name;
    global_context.id_map[native_thread_id] = id;
    global_context.contexts.emplace(id, local_context);

    return local_context;
}

void unregister_thread_impl(thread::id id)
{
    // unlock of global mutex must happen before
    // destructor of context
    std::shared_ptr<thread_context> context{};
    auto& global_context = get_global_context();
    std::lock_guard<std::mutex> lock(global_context.mutex);
    auto it = global_context.contexts.find(id);
    if(it == global_context.contexts.end())
    {
        return;
    }

    // get the context and lock it
    context = it->second;
    std::lock_guard<std::mutex> local_lock(context->tasks_mutex);

    global_context.id_map.erase(context->native_thread_id);
    // now we can safely remove the context
    // from the global container and the local variable
    // will be the last reference to it
    global_context.contexts.erase(id);

    size_t remaining_owned_threads = global_context.get_remaining_owned_threads();
    // if this was the last entry then
    // notify that everything is cleaned up
    if(remaining_owned_threads == 0)
    {
        global_context.cleanup_event.notify_all();
    }
}

void init(const init_data& data)
{
    auto& global_context = get_global_context();
    if(global_context.init_count++ != 0)
    {
        return;
    }

    this_thread::register_this_thread("Main Thread");
    std::unique_lock<std::mutex> lock(global_context.mutex);
    global_context.main_thread_id = this_thread::get_id();
    global_context.config = data;
    log_info_func("Successful.");
}

auto shutdown(const std::chrono::seconds& timeout) -> int
{
    auto& global_context = get_global_context();
    if(global_context.init_count == 0)
    {
        log_error_func("Shutting down when not initted.");
        return -1;
    }

    if(--global_context.init_count != 0)
    {
        return -1;
    }

    this_thread::unregister_this_thread();
    log_info_func("Notifying and waiting for threads to complete.");
    auto all_threads = get_all_registered_threads();
    for(const auto& id : all_threads)
    {
        notify_for_exit(id);
    }
    std::unique_lock<std::mutex> lock(global_context.mutex);

    // guard for spurious wakeups
    auto predicate = [&]() -> bool
    {
        return global_context.get_remaining_owned_threads() == 0;
    };

    auto result = global_context.cleanup_event.wait_for(lock, timeout, predicate);

    if(result)
    {
        log_info_func("Successful.");
        global_context.config = {};
        return 0;
    }
    else
    {
        size_t remaining_owned_threads = global_context.get_remaining_owned_threads();
        log_info_func("Timed out. Not all registered threads exited. Internal Threads remaining: " + std::to_string(remaining_owned_threads));
        for(const auto& p : global_context.contexts)
        {
            std::string thread_name = std::to_string(p.first);
            if(!p.second->name.empty())
            {
                thread_name += " - " + p.second->name;
            }
            if(p.second->external)
            {
                thread_name += " (External)";
            }
            log_info_func("Thread: " + thread_name + " still running.");
        }
        global_context.config = {};
        return remaining_owned_threads;
    }
}

auto get_all_registered_threads() -> std::vector<thread::id>
{
    std::vector<thread::id> result;
    auto& global_context = get_global_context();
    std::unique_lock<std::mutex> lock(global_context.mutex);

    result.reserve(global_context.contexts.size());
    for(const auto& p : global_context.contexts)
    {
        result.emplace_back(p.first);
    }

    return result;
}

// the registered context of thread id, or null
auto find_context(thread::id id) -> std::shared_ptr<thread_context>
{
    auto& global_context = get_global_context();
    std::lock_guard<std::mutex> lock(global_context.mutex);
    auto it = global_context.contexts.find(id);
    return it == global_context.contexts.end() ? nullptr : it->second;
}

auto get_pending_task_count_detailed(thread::id id) -> task_info
{
    if(id == invalid_id())
    {
        log_error_func("Invoking to an invalid thread.");
        return {};
    }
    auto context = find_context(id);
    if(!context)
    {
        return {};
    }

    std::lock_guard<std::mutex> remote_lock(context->tasks_mutex);

    const auto left_to_process = context->processing_tasks.size() - context->processing_idx +
                                 context->on_process_batch.size() - context->on_process_idx;
    const auto pending = context->tasks.size() + context->on_process_tasks.size();
    const auto processing = context->processing_stack_depth.load();
    const auto total = processing + left_to_process + pending;

    task_info info;
    info.left_to_process = left_to_process;
    info.processing = processing;
    info.pending = pending;
    info.total = total;

    if(!context->name.empty())
    {
        info.thread_name = context->name;
    }
    else
    {
        info.thread_name = std::to_string(id);
    }
    return info;
}

auto get_pending_task_count(thread::id id) -> size_t
{
    return get_pending_task_count_detailed(id).total;
}

auto has_tasks_to_process(const thread_context& context) -> bool
{
    return context.processing_idx < context.processing_tasks.size();
}

auto prepare_tasks(thread_context& context) -> bool
{
    if(!has_tasks_to_process(context) && !context.tasks.empty())
    {
        std::swap(context.tasks, context.processing_tasks);
        context.tasks.clear();
        if(context.tasks.capacity() > context.capacity_shrink_threashold)
        {
            context.tasks.shrink_to_fit();
        }
        context.processing_idx = 0;
    }

    return has_tasks_to_process(context);
}

void notify_for_exit(thread::id id)
{
    auto context = find_context(id);
    if(!context)
    {
        return;
    }
    {
        std::lock_guard<std::mutex> remote_lock(context->tasks_mutex);
        context->exit = true;
    }
    context->parker->unpark();
}

void notify(thread::id id)
{
    // An empty task, not a bare unpark: a thread that is not waiting yet still returns from its next wait.
    invoke(id,
           []()
           {
           });
}

void set_detached_executor(detached_executor executor)
{
    auto& global_context = get_global_context();
    std::lock_guard<std::mutex> lock(global_context.detached_mutex);
    global_context.detached = std::move(executor);
}

auto register_thread(std::thread::id id, const std::string& name) -> thread::id
{
    auto ctx = register_thread_impl(id, name);
    return ctx->id;
}


namespace detail
{
namespace
{
// queues the task with the thread's tasks or, for on_process, with its
// on-process tasks, and wakes the thread
auto queue_packaged_task(thread::id id, task& f, bool on_process) -> bool
{
    if(f == nullptr)
    {
        log_error_func("Invoking an invalid task.");
        return false;
    }
    if(id == invalid_id())
    {
        log_error_func("Invoking to an invalid thread.");
        return false;
    }
    auto context = find_context(id);
    if(!context)
    {
        return false;
    }
    {
        std::lock_guard<std::mutex> remote_lock(context->tasks_mutex);
        auto& queue = on_process ? context->on_process_tasks : context->tasks;
        queue.emplace_back(std::move(f));
    }
    context->parker->unpark();
    return true;
}
} // namespace

// this function exists to avoid extra moves of the functor
// via the dispatch
auto invoke_packaged_task(thread::id id, task& f) -> bool
{
    return queue_packaged_task(id, f, false);
}

auto invoke_packaged_task_on_process(thread::id id, task& f) -> bool
{
    return queue_packaged_task(id, f, true);
}

auto is_on_process_point(thread::id id) -> bool
{
    if(!has_local_context())
    {
        return false;
    }
    const auto& local_context = get_local_context();
    return local_context.id == id && local_context.on_process_depth != 0 &&
           local_context.on_process_depth == local_context.processing_stack_depth;
}

auto run_detached(task& work) -> bool
{
    detached_executor executor;
    {
        auto& global_context = get_global_context();
        std::lock_guard<std::mutex> lock(global_context.detached_mutex);
        executor = global_context.detached;
    }
    if(!executor)
    {
        return false;
    }
    executor(std::move(work));
    return true;
}

void log_info(const std::string& message)
{
    tpp::log_info(message);
}

void log_error(const std::string& message)
{
    tpp::log_error(message);
}
} // namespace detail
namespace main_thread
{
auto get_id() -> thread::id
{
    const auto& global_context = get_global_context();
    return global_context.main_thread_id;
}
} // namespace main_thread
namespace this_thread
{
namespace detail
{
auto process_one(std::unique_lock<std::mutex>& lock) -> bool
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");

        return false;
    }
    auto& local_context = get_local_context();

    if(prepare_tasks(local_context))
    {
        auto task = std::move(local_context.processing_tasks[local_context.processing_idx]);
        local_context.processing_idx++;
        local_context.processing_stack_depth++;
        lock.unlock();

        if(task)
        {
            task();

            // invoke the tasks's destructor to allow
            // invoking from it on an unlocked mutex
            task = {};
        }

        lock.lock();
        local_context.processing_stack_depth--;

        return true;
    }

    return false;
}

void process_all_for(std::unique_lock<std::mutex>& lock, const std::chrono::microseconds& rtime)
{
    auto now = clock::now();
    auto end_time = now + rtime;

    while(!notified_for_exit() && now < end_time)
    {
        if(!process_one(lock))
        {
            break;
        }

        now = clock::now();
    }
}

void process_all(std::unique_lock<std::mutex>& lock)
{
    while(!notified_for_exit())
    {
        if(!process_one(lock))
        {
            break;
        }
    }
}

// the thread's state around one on-process task, also when it throws: the
// depth marks the task and the mutex is unlocked while it runs, and the task
// is destroyed on the unlocked mutex to allow invoking from its destructor
struct on_process_task_scope
{
    on_process_task_scope(thread_context& context, std::unique_lock<std::mutex>& lock, task& work)
        : context_(context)
        , lock_(lock)
        , work_(work)
    {
        context_.processing_stack_depth++;
        context_.on_process_depth = context_.processing_stack_depth;
        lock_.unlock();
    }

    ~on_process_task_scope()
    {
        work_ = {};
        lock_.lock();
        context_.on_process_depth = 0;
        context_.processing_stack_depth--;
    }

    on_process_task_scope(const on_process_task_scope&) = delete;
    auto operator=(const on_process_task_scope&) -> on_process_task_scope& = delete;

    thread_context& context_;
    std::unique_lock<std::mutex>& lock_;
    task& work_;
};

// runs the current batch of on-process tasks until it is done or end_time;
// a new batch starts from the queued tasks only once the previous one is done,
// so tasks queued meanwhile wait for the next call. Runs nothing when called
// from inside a task.
void process_on_process(std::unique_lock<std::mutex>& lock, clock::time_point end_time)
{
    auto& local_context = get_local_context();
    if(local_context.processing_stack_depth != 0)
    {
        return;
    }

    if(local_context.on_process_idx >= local_context.on_process_batch.size())
    {
        local_context.on_process_batch.clear();
        std::swap(local_context.on_process_batch, local_context.on_process_tasks);
        local_context.on_process_idx = 0;
    }

    while(local_context.on_process_idx < local_context.on_process_batch.size() && !notified_for_exit() &&
          clock::now() < end_time)
    {
        auto work = std::move(local_context.on_process_batch[local_context.on_process_idx]);
        local_context.on_process_idx++;

        on_process_task_scope scope(local_context, lock, work);
        if(work)
        {
            work();
        }
    }
}

// this_thread::process_and_wait: the ticket is taken before the queues are
// looked at, so a task queued while the others ran is not missed. Called from
// inside a task it can run no on-process task, so those do not end the wait
// either (it would spin on them).
void process_and_wait()
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return;
    }
    auto& local_context = get_local_context();
    const auto ticket = local_context.parker->prepare();
    {
        std::unique_lock<std::mutex> lock(local_context.tasks_mutex);

        process_all(lock);
        process_on_process(lock, clock::time_point::max());

        const bool can_run_on_process = local_context.processing_stack_depth == 0;
        const bool has_on_process = !local_context.on_process_tasks.empty() ||
                                    local_context.on_process_idx < local_context.on_process_batch.size();
        if(local_context.exit || !local_context.tasks.empty() || has_tasks_to_process(local_context) ||
           (can_run_on_process && has_on_process))
        {
            return;
        }
    }
    local_context.parker->park(ticket);
}

void process_for(const std::chrono::microseconds& rtime)
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return;
    }
    auto& local_context = get_local_context();
    const auto end_time = clock::now() + rtime;

    std::unique_lock<std::mutex> lock(local_context.tasks_mutex);

    process_all_for(lock, rtime);
    process_on_process(lock, end_time);
}

void process()
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return;
    }
    auto& local_context = get_local_context();

    std::unique_lock<std::mutex> lock(local_context.tasks_mutex);

    process_all(lock);
    process_on_process(lock, clock::time_point::max());
}

auto wait_for(const std::chrono::microseconds& wait_duration) -> std::cv_status
{
    auto status = std::cv_status::no_timeout;

    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return status;
    }
    auto& local_context = get_local_context();
    const auto ticket = local_context.parker->prepare();
    {
        std::unique_lock<std::mutex> lock(local_context.tasks_mutex);
        if(process_one(lock))
        {
            return status;
        }
    }
    if(notified_for_exit())
    {
        return status;
    }

    if(!local_context.parker->park_until(ticket, clock::now() + wait_duration))
    {
        status = std::cv_status::timeout;
    }

    std::unique_lock<std::mutex> lock(local_context.tasks_mutex);
    process_one(lock);

    return status;
}

void wait()
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return;
    }
    auto& local_context = get_local_context();
    const auto ticket = local_context.parker->prepare();
    {
        std::unique_lock<std::mutex> lock(local_context.tasks_mutex);
        if(process_one(lock))
        {
            return;
        }
    }

    if(notified_for_exit())
    {
        return;
    }

    local_context.parker->park(ticket);

    std::unique_lock<std::mutex> lock(local_context.tasks_mutex);
    process_one(lock);
}
} // namespace detail

void register_this_thread()
{
    if(has_local_context())
    {
        return;
    }
    auto context = register_thread_impl(std::this_thread::get_id(), {}, false);
    set_local_context(context.get());
}

void register_this_thread(const std::string& name, bool external)
{
    if(has_local_context())
    {
        return;
    }
    auto context = register_thread_impl(std::this_thread::get_id(), name, external);
    set_local_context(context.get());
}

void unregister_this_thread()
{
    unregister_thread_impl(get_id());
    set_local_context(nullptr);
}

auto notified_for_exit() -> bool
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return true;
    }
    auto& local_context = get_local_context();

    return local_context.exit;
}

void process()
{
    detail::process();
}

void wait()
{
    detail::wait();
}

void process_and_wait()
{
    detail::process_and_wait();
}

auto get_id() -> thread::id
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return invalid_id();
    }
    auto& local_context = get_local_context();
    return local_context.id;
}

auto get_depth() -> uint32_t
{
    if(!has_local_context())
    {
        log_error_func("Calling functions in the this_thread namespace "
                       "requires the thread to be already registered by calling "
                       "this_thread::register_this_thread");
        return 0;
    }
    auto& local_context = get_local_context();
    return local_context.processing_stack_depth;
}

auto is_registered() -> bool
{
    return has_local_context();
}

} // namespace this_thread

//-----------------------------------------------------------------------------
/// The scheduler: how a blocked thread makes progress (detail/scheduler.h).
//-----------------------------------------------------------------------------
namespace detail
{
namespace
{
/// The innermost pool job running on this thread.
thread_local job_scope* current_job_scope = nullptr;
/// The pool this thread is a worker of.
thread_local std::weak_ptr<job_source> current_worker_source;
thread_local const job_source* current_worker = nullptr;

struct hosted_source
{
    std::weak_ptr<job_source> source;
    const job_source* raw{};
};
/// The pools without workers this thread hosts.
thread_local std::vector<hosted_source> hosted_sources;

/// Pool jobs running inside waits on this thread, nested.
thread_local std::uint32_t helping_depth = 0;
thread_local bool has_reported_helping_depth = false;

/// The parker of a thread that is not registered.
thread_local parker_ptr unregistered_parker;

/// How far a wait follows a future to the jobs that produce it: a continuation's
/// source, a when_all's inputs, their sources.
constexpr std::size_t dependency_depth = 8;
/// How long a wait that can run nothing to end it goes before it says so.
constexpr auto stall_report_delay = std::chrono::seconds(10);

/// A pool job run inside a wait, for as long as it runs.
struct helping_scope
{
    helping_scope()
    {
        ++helping_depth;
    }
    ~helping_scope()
    {
        --helping_depth;
    }
    helping_scope(const helping_scope&) = delete;
    auto operator=(const helping_scope&) -> helping_scope& = delete;
};

auto may_run_jobs_in_wait() -> bool
{
    if(helping_depth < max_helping_depth)
    {
        return true;
    }
    if(!has_reported_helping_depth)
    {
        has_reported_helping_depth = true;
        tpp::log_error("[tpp] Waits nested " + std::to_string(max_helping_depth) +
                       " jobs deep on one thread: deeper waits run no job and only wait.");
    }
    return false;
}

/// The pools the calling thread may help: the one it works for and the ones it hosts.
auto get_helpable_sources() -> std::vector<std::shared_ptr<job_source>>
{
    std::vector<std::shared_ptr<job_source>> sources;
    if(auto worker = current_worker_source.lock())
    {
        sources.push_back(std::move(worker));
    }
    hosted_sources.erase(std::remove_if(hosted_sources.begin(),
                                        hosted_sources.end(),
                                        [](const hosted_source& hosted)
                                        {
                                            return hosted.source.expired();
                                        }),
                         hosted_sources.end());
    for(const auto& hosted : hosted_sources)
    {
        if(auto source = hosted.source.lock())
        {
            sources.push_back(std::move(source));
        }
    }
    return sources;
}

/// Adds the pools that run the jobs the target depends on: an untimed wait may run those jobs once every
/// worker is busy, so a worker taking a job wakes it.
void add_dependency_sources(const wait_target& target, std::vector<std::shared_ptr<job_source>>& sources)
{
    std::vector<job_link> jobs;
    target.collect_jobs(jobs, dependency_depth);
    for(const auto& link : jobs)
    {
        auto source = link.source.lock();
        if(source && std::find(sources.begin(), sources.end(), source) == sources.end())
        {
            sources.push_back(std::move(source));
        }
    }
}

auto run_one_task(thread_context& context) -> bool
{
    std::unique_lock<std::mutex> lock(context.tasks_mutex);
    return this_thread::detail::process_one(lock);
}

/// Runs one queued job the target depends on, when the calling thread may.
auto run_dependency(const wait_target& target, bool is_timed) -> bool
{
    if(!may_run_jobs_in_wait())
    {
        return false;
    }
    std::vector<job_link> jobs;
    target.collect_jobs(jobs, dependency_depth);
    for(const auto& link : jobs)
    {
        auto source = link.source.lock();
        if(!source)
        {
            continue;
        }
        helping_scope scope;
        if(source->run_waited_job(link.id, is_timed))
        {
            return true;
        }
    }
    return false;
}

/// Runs one queued job of a pool the calling thread may help, when nothing else would run it.
auto run_help(const std::vector<std::shared_ptr<job_source>>& sources, bool is_timed) -> bool
{
    if(sources.empty() || !may_run_jobs_in_wait())
    {
        return false;
    }
    for(const auto& source : sources)
    {
        helping_scope scope;
        if(source->run_job_to_help(is_timed))
        {
            return true;
        }
    }
    return false;
}

/// The waiter and helper registrations of one wait, undone however it ends.
struct wait_registration
{
    wait_registration(wait_target& target, parker_ptr parker, std::vector<std::shared_ptr<job_source>> sources)
        : target_(target)
        , parker_(std::move(parker))
        , sources_(std::move(sources))
    {
        target_.add_waiter(parker_);
        for(const auto& source : sources_)
        {
            source->add_parked_helper(parker_);
        }
    }

    ~wait_registration()
    {
        for(const auto& source : sources_)
        {
            source->remove_parked_helper(parker_);
        }
        target_.remove_waiter(parker_);
    }

    wait_registration(const wait_registration&) = delete;
    auto operator=(const wait_registration&) -> wait_registration& = delete;

    wait_target& target_;
    parker_ptr parker_;
    std::vector<std::shared_ptr<job_source>> sources_;
};

/// What the calling thread runs and waits in, for a report.
auto describe_current_jobs() -> std::string
{
    std::string jobs;
    for(auto* scope = current_job_scope; scope != nullptr; scope = scope->outer)
    {
        jobs += jobs.empty() ? "" : ", inside ";
        jobs += scope->source->describe_job(scope->id);
    }
    return jobs.empty() ? "no job" : jobs;
}

void report_stall(const wait_target& target)
{
    std::string thread_name = "a thread that is not registered";
    if(has_local_context())
    {
        const auto& context = get_local_context();
        thread_name = context.name.empty() ? "thread " + std::to_string(context.id) : context.name;
    }
    tpp::log_error("[tpp] " + thread_name + " has waited " + std::to_string(stall_report_delay.count()) +
                   " s for " + target.describe() + " (running " + describe_current_jobs() +
                   ") with nothing it may run to end the wait.");
}

/// Never ready: a sleep is a wait for its deadline.
struct deadline_target final : wait_target
{
    auto is_ready() const -> bool override
    {
        return false;
    }
    void add_waiter(const parker_ptr& /*waiter*/) override
    {
    }
    void remove_waiter(const parker_ptr& /*waiter*/) override
    {
    }
    auto describe() const -> std::string override
    {
        return "a deadline";
    }
};
} // namespace

void wait_target::collect_jobs(std::vector<job_link>& /*jobs*/, std::size_t /*depth*/) const
{
}

auto wait_target::describe() const -> std::string
{
    return "a signal";
}

auto current_parker() -> parker_ptr
{
    if(has_local_context())
    {
        return get_local_context().parker;
    }
    if(!unregistered_parker)
    {
        unregistered_parker = std::make_shared<parker>();
    }
    return unregistered_parker;
}

auto wait_for_target(wait_target& target, bool is_timed, clock::time_point deadline) -> wait_status
{
    // The job the target waits for, when this thread may run it at once (always in a pool without workers):
    // nothing to register for then.
    if(run_dependency(target, is_timed) && target.is_ready())
    {
        return wait_status::ready;
    }
    thread_context* context = has_local_context() ? &get_local_context() : nullptr;
    const auto parker = current_parker();
    auto sources = get_helpable_sources();
    if(!is_timed)
    {
        add_dependency_sources(target, sources);
    }
    // A stall is reported only where nothing else could end the wait: a host of a pool without workers.
    const bool reports_stall = !is_timed && !hosted_sources.empty();
    wait_registration registration(target, parker, sources);
    auto last_progress = clock::now();
    bool has_reported_stall = false;
    // A wait that is out of time still makes one attempt: a zero timeout runs a job a pool without workers holds.
    bool is_first_attempt = true;
    for(;;)
    {
        const auto ticket = parker->prepare();
        if(target.is_ready())
        {
            return wait_status::ready;
        }
        if(context != nullptr && context->exit)
        {
            return wait_status::exit;
        }
        if(!is_first_attempt && clock::now() >= deadline)
        {
            return wait_status::timeout;
        }
        is_first_attempt = false;
        // The thread's own tasks first, as every wait always ran them; then the work this wait depends on;
        // then other queued jobs, only where no other thread would run them.
        if((context != nullptr && run_one_task(*context)) || run_dependency(target, is_timed) ||
           run_help(registration.sources_, is_timed))
        {
            last_progress = clock::now();
            continue;
        }
        if(clock::now() >= deadline)
        {
            return target.is_ready() ? wait_status::ready : wait_status::timeout;
        }
        auto wake = deadline;
        if(reports_stall && !has_reported_stall)
        {
            wake = std::min(deadline, last_progress + stall_report_delay);
        }
        if(wake == clock::time_point::max())
        {
            parker->park(ticket);
            continue;
        }
        if(!parker->park_until(ticket, wake) && reports_stall && !has_reported_stall &&
           clock::now() >= last_progress + stall_report_delay && !target.is_ready())
        {
            has_reported_stall = true;
            report_stall(target);
        }
    }
}

job_scope::job_scope(const job_source* source, std::uint64_t id)
    : source(source)
    , id(id)
    , outer(current_job_scope)
{
    current_job_scope = this;
    // A job is a task of the thread it runs on: a process() inside it runs no on-process task.
    if(has_local_context())
    {
        get_local_context().processing_stack_depth++;
    }
}

job_scope::~job_scope()
{
    if(has_local_context())
    {
        get_local_context().processing_stack_depth--;
    }
    current_job_scope = outer;
}

auto current_job_of(const job_source* source) -> std::uint64_t
{
    for(auto* scope = current_job_scope; scope != nullptr; scope = scope->outer)
    {
        if(scope->source == source)
        {
            return scope->id;
        }
    }
    return 0;
}

auto is_running_here(const job_source* source, std::uint64_t id) -> bool
{
    for(auto* scope = current_job_scope; scope != nullptr; scope = scope->outer)
    {
        if(scope->source == source && scope->id == id)
        {
            return true;
        }
    }
    return false;
}

worker_scope::worker_scope(const std::weak_ptr<job_source>& source)
{
    current_worker_source = source;
    current_worker = source.lock().get();
}

worker_scope::~worker_scope()
{
    current_worker_source.reset();
    current_worker = nullptr;
}

auto is_worker_of(const job_source* source) -> bool
{
    return source != nullptr && current_worker == source;
}

void add_hosted_source(const std::weak_ptr<job_source>& source)
{
    hosted_sources.push_back({source, source.lock().get()});
}

auto is_host_of(const job_source* source) -> bool
{
    return std::any_of(hosted_sources.begin(),
                       hosted_sources.end(),
                       [source](const hosted_source& hosted)
                       {
                           return hosted.raw == source && !hosted.source.expired();
                       });
}

auto make_thread_running(const std::string& name, task body) -> thread
{
    thread t(name,
             [name, body = std::move(body)]()
             {
                 name_thread(name);

                 this_thread::register_this_thread(name);

                 on_thread_start(name);

                 body();

                 this_thread::unregister_this_thread();
             });

    return t;
}
} // namespace detail

namespace this_thread
{
namespace detail
{
void sleep_until(clock::time_point deadline)
{
    if(!has_local_context())
    {
        std::this_thread::sleep_until(deadline);
        return;
    }
    tpp::detail::deadline_target target;
    tpp::detail::wait_for_target(target, true, deadline);
}
} // namespace detail
} // namespace this_thread

auto make_thread(const std::string& name) -> thread
{
    return detail::make_thread_running(name,
                                       []()
                                       {
                                           while(!this_thread::notified_for_exit())
                                           {
                                               this_thread::process_and_wait();
                                           }
                                       });
}

auto make_shared_thread(const std::string& name) -> shared_thread
{
    return std::make_shared<thread>(make_thread(name));
}

auto thread::get_id() const -> thread::id
{
    return id_;
}

void thread::join()
{
    notify_for_exit(get_id());
    std::thread::join();
}

void thread::register_this(const std::string& name)
{
    auto context = register_thread_impl(std::thread::get_id(), name);
    id_ = context->id;
}

thread::thread() noexcept = default;

void thread::swap(thread& th) noexcept
{
    std::swap(static_cast<std::thread&>(*this), static_cast<std::thread&>(th));
    std::swap(id_, th.id_);
}

thread& thread::operator=(thread&& th) noexcept
{
    if(joinable())
    {
        join();
    }
    swap(th);
    return *this;
}

thread::~thread()
{
    if(joinable())
    {
        join();
    }
}

auto set_thread_config(thread::id id, tasks_capacity_config config) -> bool
{
    return tpp::dispatch(id,
                         [config]()
                         {
                             auto& local_context = get_local_context();
                             std::lock_guard<std::mutex> lock(local_context.tasks_mutex);
                             local_context.tasks.reserve(config.default_reserved_tasks);
                             local_context.capacity_shrink_threashold = config.capacity_shrink_threashold;
                         });
}

} // namespace tpp
