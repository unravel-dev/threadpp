#pragma once
#include "scheduler.h"

#include <algorithm>
#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

namespace tpp
{

namespace detail
{

enum class value_status : unsigned
{
    not_set,
    ready,
    error
};

//-----------------------------------------------------------------------------
/// What every future's shared state holds, whatever its value type: whether it
/// is ready, the continuations to run then, the threads blocked on it, and links
/// to the work that produces it - its pool job, and the states it is made from
/// (a continuation's source, a when_all's inputs) - which a waiting thread may run
/// itself (scheduler.h).
//-----------------------------------------------------------------------------
struct state_base : wait_target
{
    using callback_container = std::vector<task>;

    mutable std::mutex guard;
    callback_container callbacks;
    std::vector<parker_ptr> waiters;
    job_link job;
    std::vector<std::weak_ptr<state_base>> antecedents;

    std::exception_ptr exception;

    std::atomic<value_status> status = {value_status::not_set};
    std::atomic_flag retrieved = ATOMIC_FLAG_INIT;

    state_base() = default;
    state_base(const state_base&) = delete;
    auto operator=(const state_base&) -> state_base& = delete;
    ~state_base() = default;

    bool ready() const
    {
        return status != value_status::not_set;
    }

    bool has_error() const
    {
        return status == value_status::error;
    }

    void set_exception(std::exception_ptr ex)
    {
        std::unique_lock<std::mutex> lock(guard);

        if(ready())
        {
            throw std::future_error(std::future_errc::promise_already_satisfied);
        }
        exception = std::move(ex);
        set_ready(lock, value_status::error);
    }

    //-----------------------------------------------------------------------------
    /// Makes the state ready, wakes the threads blocked on it and runs its
    /// continuations, on the calling thread, after the lock is released.
    //-----------------------------------------------------------------------------
    void set_ready(std::unique_lock<std::mutex>& lock, value_status s)
    {
        callback_container continuations;
        std::vector<parker_ptr> parked;
        {
            status = s;
            continuations = std::move(callbacks);
            callbacks.clear();
            parked = std::move(waiters);
            waiters.clear();
            job = {};
            antecedents.clear();
        }

        lock.unlock();
        for(const auto& waiter : parked)
        {
            waiter->unpark();
        }
        for(const auto& continuation : continuations)
        {
            if(continuation)
            {
                continuation();
            }
        }
    }

    void set_continuation(task continuation)
    {
        {
            std::lock_guard<std::mutex> lock(guard);
            if(!ready())
            {
                callbacks.emplace_back(std::move(continuation));
                return;
            }
        }

        if(continuation)
        {
            continuation();
        }
    }

    //-----------------------------------------------------------------------------
    /// The pool job that sets this state.
    //-----------------------------------------------------------------------------
    void set_job(job_link link)
    {
        std::lock_guard<std::mutex> lock(guard);
        if(!ready())
        {
            job = std::move(link);
        }
    }

    //-----------------------------------------------------------------------------
    /// A state this one is made from: waiting for this one runs its producers too.
    //-----------------------------------------------------------------------------
    void add_antecedent(const std::shared_ptr<state_base>& state)
    {
        std::lock_guard<std::mutex> lock(guard);
        if(!ready() && state)
        {
            antecedents.emplace_back(state);
        }
    }

    auto is_ready() const -> bool override
    {
        return ready();
    }

    void add_waiter(const parker_ptr& waiter) override
    {
        {
            std::lock_guard<std::mutex> lock(guard);
            if(!ready())
            {
                waiters.push_back(waiter);
                return;
            }
        }
        // Ready meanwhile: the waiter's next check sees it.
        waiter->unpark();
    }

    void remove_waiter(const parker_ptr& waiter) override
    {
        std::lock_guard<std::mutex> lock(guard);
        auto it = std::find(waiters.begin(), waiters.end(), waiter);
        if(it != waiters.end())
        {
            waiters.erase(it);
        }
    }

    void collect_jobs(std::vector<job_link>& jobs, std::size_t depth) const override
    {
        job_link own;
        std::vector<std::weak_ptr<state_base>> sources;
        {
            std::lock_guard<std::mutex> lock(guard);
            if(ready())
            {
                return;
            }
            own = job;
            if(depth > 0)
            {
                sources = antecedents;
            }
        }
        if(own.id != 0)
        {
            jobs.push_back(own);
        }
        for(const auto& weak_source : sources)
        {
            if(auto source = weak_source.lock())
            {
                source->collect_jobs(jobs, depth - 1);
            }
        }
    }

    auto describe() const -> std::string override
    {
        job_link own;
        {
            std::lock_guard<std::mutex> lock(guard);
            own = job;
        }
        if(auto source = own.source.lock())
        {
            return source->describe_job(own.id);
        }
        return "a future";
    }

    void wait()
    {
        // The status turns ready last, under the lock, after the value or exception is stored.
        if(ready())
        {
            return;
        }
        wait_for_target(*this, false, clock::time_point::max());
    }

    template<typename Rep, typename Per>
    auto wait_for(const std::chrono::duration<Rep, Per>& timeout_duration) -> std::future_status
    {
        if(ready())
        {
            return std::future_status::ready;
        }
        const auto now = clock::now();
        const auto duration = std::chrono::duration_cast<clock::duration>(timeout_duration);
        const auto deadline = duration >= clock::time_point::max() - now ? clock::time_point::max() : now + duration;
        switch(wait_for_target(*this, true, deadline))
        {
            case wait_status::ready:
                return std::future_status::ready;
            case wait_status::exit:
                return std::future_status::deferred;
            case wait_status::timeout:
            default:
                return ready() ? std::future_status::ready : std::future_status::timeout;
        }
    }

    void rethrow_any_exception() const
    {
        if(exception)
        {
            std::rethrow_exception(exception);
        }
    }
};

template<typename T>
struct basic_state : state_base
{
};

template<typename T>
struct future_state final : public basic_state<T>
{
    std::shared_ptr<T> value;

    template<typename V>
    void set_value(V&& val)
    {
        std::unique_lock<std::mutex> lock(this->guard);

        if(this->ready())
        {
            throw std::future_error(std::future_errc::promise_already_satisfied);
        }

        this->value = std::make_shared<std::decay_t<V>>(std::forward<V>(val));
        this->set_ready(lock, value_status::ready);
    }

    auto get_value_assuming_ready() -> decltype(auto)
    {
        // A ready state never stores a value or exception again, so it reads without the lock.
        std::unique_lock<std::mutex> lock(this->guard, std::defer_lock);
        if(!this->ready())
        {
            lock.lock();
        }

        if(value)
        {
            return *value;
        }

        if(this->exception)
        {
            std::rethrow_exception(this->exception);
        }
        else
        {
            throw std::future_error(std::future_errc::broken_promise);
        }
    }
};

template<>
struct future_state<void> final : public basic_state<void>
{
    void set_value()
    {
        std::unique_lock<std::mutex> lock(guard);

        if(ready())
        {
            throw std::future_error(std::future_errc::promise_already_satisfied);
        }

        set_ready(lock, value_status::ready);
    }

    void get_value_assuming_ready()
    {
        rethrow_any_exception();
    }
};
template<typename T>
inline void check_state(const std::shared_ptr<future_state<T>>& state)
{
    if(!state)
    {
        throw std::future_error(std::future_errc::no_state);
    }
}

template<typename T>
inline void state_invalidate(std::shared_ptr<future_state<T>>& state)
{
    state.reset();
}
} // namespace detail
} // namespace tpp
