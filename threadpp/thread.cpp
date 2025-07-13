#include "thread.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
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

    std::condition_variable wakeup_event;
    std::atomic<std::uint32_t> processing_stack_depth{0};

    std::string name;
    std::atomic<bool> wakeup{false};
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

auto register_thread_impl(std::thread::id native_thread_id, const std::string& name) -> std::shared_ptr<thread_context>
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
    // if this was the last entry then
    // notify that everything is cleaned up
    if(global_context.contexts.empty())
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
        return global_context.contexts.empty();
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
        log_info_func("Timed out. Not all registered threads exited.");
        global_context.config = {};
        return static_cast<int>(global_context.contexts.size());
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

auto get_pending_task_count_detailed(thread::id id) -> task_info
{
    if(id == invalid_id())
    {
        log_error_func("Invoking to an invalid thread.");
        return {};
    }
    auto& global_context = get_global_context();
    std::unique_lock<std::mutex> lock(global_context.mutex);

    auto it = global_context.contexts.find(id);
    if(it == global_context.contexts.end())
    {
        return {};
    }

    auto context = it->second;

    lock.unlock();

    std::lock_guard<std::mutex> remote_lock(context->tasks_mutex);

    const auto left_to_process = context->processing_tasks.size() - context->processing_idx;
    const auto pending = context->tasks.size();
    const auto processing = context->processing_stack_depth.load();
    const auto total = processing + left_to_process + pending;

    task_info info;
    info.left_to_process = left_to_process;
    info.processing = processing;
    info.pending = pending;
    info.total = total;

    if(info.total > 200)
    {
        int a = 0;
        a++;
    }

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
    auto& global_context = get_global_context();
    std::unique_lock<std::mutex> lock(global_context.mutex);

    auto it = global_context.contexts.find(id);
    if(it == global_context.contexts.end())
    {
        return;
    }

    auto context = it->second;

    lock.unlock();

    std::lock_guard<std::mutex> remote_lock(context->tasks_mutex);

    context->exit = true;
    context->wakeup = true;
    context->wakeup_event.notify_all();
}

void notify(thread::id id)
{
    invoke(id,
           []()
           {
           });
}


auto register_thread(std::thread::id id, const std::string& name) -> thread::id
{
    auto ctx = register_thread_impl(id, name);
    return ctx->id;
}


namespace detail
{
// this function exists to avoid extra moves of the functor
// via the dispatch
auto invoke_packaged_task(thread::id id, task& f) -> bool
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
    auto& global_context = get_global_context();
    std::unique_lock<std::mutex> lock(global_context.mutex);

    auto it = global_context.contexts.find(id);
    if(it == global_context.contexts.end())
    {
        return false;
    }

    auto context = it->second;

    lock.unlock();

    std::lock_guard<std::mutex> remote_lock(context->tasks_mutex);

    context->tasks.emplace_back(std::move(f));
    context->wakeup = true;
    context->wakeup_event.notify_all();
    return true;
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

    std::unique_lock<std::mutex> lock(local_context.tasks_mutex);

    process_all_for(lock, rtime);
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

    std::unique_lock<std::mutex> lock(local_context.tasks_mutex);

    if(process_one(lock))
    {
        return status;
    }
    if(notified_for_exit())
    {
        return status;
    }

    // guard for spurious wakeups
    auto predicate = [&]() -> bool
    {
        return local_context.wakeup;
    };

    local_context.wakeup = false;

    if(!local_context.wakeup_event.wait_for(lock, wait_duration, predicate))
    {
        status = std::cv_status::timeout;
    }

    local_context.wakeup = false;

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

    std::unique_lock<std::mutex> lock(local_context.tasks_mutex);

    if(process_one(lock))
    {
        return;
    }

    if(notified_for_exit())
    {
        return;
    }

    // guard for spurious wakeups
    auto predicate = [&]() -> bool
    {
        return local_context.wakeup;
    };

    local_context.wakeup = false;
    // guard for spurious wakeups
    local_context.wakeup_event.wait(lock, predicate);

    local_context.wakeup = false;

    process_one(lock);
}
} // namespace detail

void register_this_thread()
{
    auto context = register_thread_impl(std::this_thread::get_id(), {});
    set_local_context(context.get());
}

void register_this_thread(const std::string& name)
{
    auto context = register_thread_impl(std::this_thread::get_id(), name);
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

auto make_thread(const std::string& name) -> thread
{
    thread t(name,
             [name]()
             {
                 name_thread(name);

                 this_thread::register_this_thread(name);

                 on_thread_start(name);

                 while(!this_thread::notified_for_exit())
                 {
                     this_thread::wait();
                 }

                 this_thread::unregister_this_thread();
             });


    return t;
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
