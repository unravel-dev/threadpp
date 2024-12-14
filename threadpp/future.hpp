#pragma once
#include "detail/future_state.hpp"
#include "detail/utility/apply.hpp"
#include "detail/utility/capture.hpp"
#include "detail/utility/invoke.hpp"

#include "thread.h"
#include <future>

namespace tpp
{

template<typename T>
class future;
template<typename T>
class shared_future;
template<typename T>
class promise;

template<typename F, typename... Args>
using callable_ret_type = utility::invoke_result_t<std::decay_t<F>, Args...>;

template<typename F, typename... Args>
using async_ret_type = callable_ret_type<F, Args...>;

template<typename F, typename T>
using then_ret_type = callable_ret_type<F, T>;

//-----------------------------------------------------------------------------
/// The template function async runs the function f a
/// synchronously (potentially in a separate thread )
/// and returns a std::future that will eventually hold
/// the result of that function call. Launch policy async
/// means that the task will be queued regardless if the
/// calling thread is the same as the destination thread.
/// Policy deferred will only queue it if the calling thread
/// and the destination thread are different otherwise it
/// will immediately execute the task.
//-----------------------------------------------------------------------------
template<typename F, typename... Args>
auto async(thread::id id, std::launch policy, F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>;
template<typename F, typename... Args>
auto async(std::launch policy, F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>;

//-----------------------------------------------------------------------------
/// The template function async runs the function f a
/// synchronously (potentially in a separate thread )
/// and returns a std::future that will eventually hold
/// the result of that function call.
//-----------------------------------------------------------------------------
template<typename F, typename... Args>
auto async(thread::id id, F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>;
template<typename F, typename... Args>
auto async(F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>;

//-----------------------------------------------------------------------------
/// produces a future that is ready immediately
/// and holds the given value
//-----------------------------------------------------------------------------
template<typename T>
auto make_ready_future(T&& value) -> future<T>;

//-----------------------------------------------------------------------------
/// produces a future that is ready immediately
/// and holds the given exception
//-----------------------------------------------------------------------------
template<class T>
auto make_exceptional_future(std::exception_ptr ex) -> future<T>;

//-----------------------------------------------------------------------------
/// produces a future that is ready immediately
/// and holds the given exception
//-----------------------------------------------------------------------------
template<class T, class E>
auto make_exceptional_future(E ex) -> future<T>;

namespace detail
{

template<typename T>
class basic_promise;

template<typename T>
class basic_future
{
    friend class basic_promise<T>;

public:
    using state_type = std::shared_ptr<future_state<T>>;

    basic_future() = default;
    basic_future(basic_future&& rhs) noexcept = default;
    auto operator=(basic_future&& rhs) noexcept -> basic_future& = default;

    basic_future(const basic_future&) = default;
    auto operator=(const basic_future&) -> basic_future& = default;

    //-----------------------------------------------------------------------------
    /// Checks if the future has an associated shared state.
    /// This is the case only for futures that were not default-constructed or
    /// moved from (i.e. returned by promise::get_future()
    //-----------------------------------------------------------------------------
    auto valid() const -> bool
    {
        return state_ != nullptr;
    }

    //-----------------------------------------------------------------------------
    /// Checks whether the result is ready. If either a value or
    /// exception was set this will return true.
    //-----------------------------------------------------------------------------
    auto is_ready() const -> bool
    {
        check_state(state_);

        return state_->ready();
    }

    //-----------------------------------------------------------------------------
    /// Checks whether an error occurred.
    //-----------------------------------------------------------------------------
    auto has_error() const -> bool
    {
        check_state(state_);

        return state_->has_error();
    }

    //-----------------------------------------------------------------------------
    /// Waits for the result to become available
    //-----------------------------------------------------------------------------
    void wait() const
    {
        check_state(state_);

        return state_->wait();
    }

    //-----------------------------------------------------------------------------
    /// Waits for the result to become available.
    /// Blocks until specified timeout_duration has elapsed or
    /// the result becomes available, whichever comes first.
    /// Returns value identifies the state of the result.
    /// This function may block for longer than timeout_duration
    /// due to scheduling or resource contention delays.
    //-----------------------------------------------------------------------------
    template<typename Rep, typename Per>
    auto wait_for(const std::chrono::duration<Rep, Per>& timeout_duration) const -> std::future_status
    {
        check_state(state_);

        return state_->wait_for(timeout_duration);
    }

    template<typename Clock, typename Duration>
    auto wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const -> std::future_status
    {
        return wait_for(abs_time.time_since_epoch() - Clock::now().time_since_epoch());
    }

    auto _internal_get_state() const -> const state_type&
    {
        return state_;
    }
    void _internal_set_state(const state_type& state)
    {
        state_ = state;
    }

protected:
    basic_future(const state_type& state) : state_(state)
    {
    }

    /// The shared state
    state_type state_;
};

template<typename T>
class basic_promise
{
public:
    basic_promise() = default;
    basic_promise(basic_promise&& rhs) noexcept = default;
    auto operator=(basic_promise&& rhs) noexcept -> basic_promise& = default;

    basic_promise(const basic_promise&) = delete;
    auto operator=(const basic_promise&) -> basic_promise& = delete;

    ~basic_promise()
    {
        if(state_ && !state_->ready())
        {
            abandon();
        }
    }

    //-----------------------------------------------------------------------------
    /// Atomically stores the exception pointer p into the shared state and makes
    /// the state ready. An exception is thrown if there is no shared state or the
    /// shared state already stores a value or exception.
    //-----------------------------------------------------------------------------
    void set_exception(std::exception_ptr p)
    {
        check_state(state_);
        state_->set_exception(std::move(p));
    }

    //-----------------------------------------------------------------------------
    /// Abandons the associated future.
    //-----------------------------------------------------------------------------
    void abandon()
    {
        auto exception = std::make_exception_ptr(std::future_error(std::future_errc::broken_promise));
        set_exception(exception);
    }

    //-----------------------------------------------------------------------------
    /// Returns a future associated with the promised result
    //-----------------------------------------------------------------------------
    auto get_future() -> future<T>
    {
        check_retrieved_flag();

        future<T> f;
        f.state_ = state_;
        return f;
    }

protected:
    void check_retrieved_flag()
    {
        check_state(state_);

        if(state_->retrieved.test_and_set())
        {
            throw std::future_error(std::future_errc::future_already_retrieved);
        }
    }

    /// The shared state
    std::shared_ptr<future_state<T>> state_ = std::make_shared<future_state<T>>();
};
} // namespace detail

template<typename T>
class promise : public detail::basic_promise<T>
{
public:
    //-----------------------------------------------------------------------------
    /// Sets the result to specific value
    //-----------------------------------------------------------------------------
    void set_value(T&& value)
    {
        check_state(this->state_);
        this->state_->set_value(std::move(value));
    }
    //-----------------------------------------------------------------------------
    /// Sets the result to specific value
    //-----------------------------------------------------------------------------
    void set_value(const T& value)
    {
        check_state(this->state_);
        this->state_->set_value(value);
    }
};
template<>
class promise<void> : public detail::basic_promise<void>
{
public:
    //-----------------------------------------------------------------------------
    /// Sets the result to specific value
    //-----------------------------------------------------------------------------
    void set_value()
    {
        check_state(this->state_);
        this->state_->set_value();
    }
};

template<typename T>
class future : public detail::basic_future<T>
{
    using base_type = detail::basic_future<T>;
    using base_type::base_type;

public:
    future() = default;
    future(future&& rhs) noexcept = default;
    auto operator=(future&& rhs) noexcept -> future& = default;

    future(const future&) = delete;
    auto operator=(const future&) -> future& = delete;

    //-----------------------------------------------------------------------------
    /// The get method waits until the future has a valid
    /// result and (depending on which template is used) retrieves it.
    /// It effectively calls wait() in order to wait for the result.
    /// The behavior is undefined if valid() is false before the call to this
    /// function. Any shared state is released. valid() is false after a call to
    /// this method.
    //-----------------------------------------------------------------------------
    auto get() -> T
    {
        this->wait();
        auto state = std::move(this->state_);
        return std::move(state->get_value_assuming_ready());
    }

    //-----------------------------------------------------------------------------
    /// Transfers the shared state of *this, if any, to a shared_future object.
    /// Multiple std::shared_future objects may reference the same shared state,
    /// which is not possible with std::future.
    /// After calling share on a std::future, valid() == false.
    //-----------------------------------------------------------------------------
    auto share() -> shared_future<T>;

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    /// After this function returns, valid() is false.
    /// Launch policy asyncmeans that the continuation will be queued
    /// regardless if the calling thread is the same as the destination thread.
    /// Policy deferred will only queue it if the calling thread
    /// and the destination thread are different otherwise it
    /// will immediately execute the continuation when the future is ready.
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, std::launch policy, F&& f) -> future<then_ret_type<F, future<T>>>;
    template<typename F>
    auto then(std::launch policy, F&& f) -> future<then_ret_type<F, future<T>>>;

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    /// After this function returns, valid() is false.
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, F&& f) -> future<then_ret_type<F, future<T>>>;
    template<typename F>
    auto then(F&& f) -> future<then_ret_type<F, future<T>>>;
};

template<>
class future<void> : public detail::basic_future<void>
{
    using base_type = detail::basic_future<void>;
    using base_type::base_type;

public:
    future() = default;
    future(future&& rhs) noexcept = default;
    auto operator=(future&& rhs) noexcept -> future& = default;

    future(const future&) = delete;
    auto operator=(const future&) -> future& = delete;

    //-----------------------------------------------------------------------------
    /// The get method waits until the future has a valid
    /// result and (depending on which template is used) retrieves it.
    /// It effectively calls wait() in order to wait for the result.
    /// The behavior is undefined if valid() is false before the call to this
    /// function. Any shared state is released. valid() is false after a call to
    /// this method.
    //-----------------------------------------------------------------------------
    void get()
    {
        this->wait();
        auto state = std::move(this->state_);
        state->get_value_assuming_ready();
    }

    //-----------------------------------------------------------------------------
    /// Transfers the shared state of *this, if any, to a shared_future object.
    /// Multiple std::shared_future objects may reference the same shared state,
    /// which is not possible with std::future.
    /// After calling share on a std::future, valid() == false.
    //-----------------------------------------------------------------------------
    auto share() -> shared_future<void>;

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    /// After this function returns, valid() is false.
    /// Launch policy asyncmeans that the continuation will be queued
    /// regardless if the calling thread is the same as the destination thread.
    /// Policy deferred will only queue it if the calling thread
    /// and the destination thread are different otherwise it
    /// will immediately execute the continuation when the future is ready.
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, std::launch policy, F&& f) -> future<then_ret_type<F, future<void>>>;
    template<typename F>
    auto then(std::launch policy, F&& f) -> future<then_ret_type<F, future<void>>>;

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    /// After this function returns, valid() is false.
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, F&& f) -> future<then_ret_type<F, future<void>>>;
    template<typename F>
    auto then(F&& f) -> future<then_ret_type<F, future<void>>>;
};

//-----------------------------------------------------------------------------
/// The class template 'shared_future' provides a mechanism
/// to access the result of asynchronous operations, similar
/// to 'future', except that multiple threads are allowed to
/// wait for the same shared state. Unlike 'future',
/// which is only moveable (so only one instance can refer
/// to any particular asynchronous result), 'shared_future' is copyable
/// and multiple shared future objects may refer to the same shared state.
/// Access to the same shared state from multiple threads is
/// safe if each thread does it through its own copy of a 'shared_future'
/// object.
//-----------------------------------------------------------------------------
template<typename T>
class shared_future : public detail::basic_future<T>
{
    using base_type = detail::basic_future<T>;
    using base_type::base_type;

public:
    shared_future() noexcept = default;
    shared_future(const shared_future& sf) = default;

    shared_future(future<T>&& uf) noexcept : base_type(std::move(uf))
    {
    }

    shared_future(shared_future&& sf) noexcept = default;
    auto operator=(const shared_future& sf) -> shared_future& = default;
    auto operator=(shared_future&& sf) noexcept -> shared_future& = default;

    //-----------------------------------------------------------------------------
    /// The get method waits until the future has a valid
    /// result and (depending on which template is used) retrieves it.
    /// It effectively calls wait() in order to wait for the result.
    /// The behavior is undefined if valid() is false before the call to this
    /// function.
    //-----------------------------------------------------------------------------
    auto get() const -> const T&
    {
        this->wait();
        return this->state_->get_value_assuming_ready();
    }

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    /// Launch policy asyncmeans that the continuation will be queued
    /// regardless if the calling thread is the same as the destination thread.
    /// Policy deferred will only queue it if the calling thread
    /// and the destination thread are different otherwise it
    /// will immediately execute the continuation when the future is ready.
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, std::launch policy, F&& f) const -> future<then_ret_type<F, shared_future<T>>>;
    template<typename F>
    auto then(std::launch policy, F&& f) const -> future<then_ret_type<F, shared_future<T>>>;

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, F&& f) const -> future<then_ret_type<F, shared_future<T>>>;
    template<typename F>
    auto then(F&& f) const -> future<then_ret_type<F, shared_future<T>>>;
};

//-----------------------------------------------------------------------------
/// The class template 'shared_future' provides a mechanism
/// to access the result of asynchronous operations, similar
/// to 'future', except that multiple threads are allowed to
/// wait for the same shared state. Unlike 'future',
/// which is only moveable (so only one instance can refer
/// to any particular asynchronous result), 'shared_future' is copyable
/// and multiple shared future objects may refer to the same shared state.
/// Access to the same shared state from multiple threads is
/// safe if each thread does it through its own copy of a 'shared_future'
/// object.
//-----------------------------------------------------------------------------
template<>
class shared_future<void> : public detail::basic_future<void>
{
    using base_type = detail::basic_future<void>;
    using base_type::base_type;

public:
    shared_future() noexcept = default;
    shared_future(const shared_future& sf) = default;
    shared_future(future<void>&& uf) noexcept : base_type(std::move(uf))
    {
    }

    shared_future(shared_future&& sf) noexcept = default;
    auto operator=(const shared_future& sf) -> shared_future& = default;
    auto operator=(shared_future&& sf) noexcept -> shared_future& = default;

    //-----------------------------------------------------------------------------
    /// The get method waits until the future has a valid
    /// result and (depending on which template is used) retrieves it.
    /// It effectively calls wait() in order to wait for the result.
    /// The behavior is undefined if valid() is false before the call to this
    /// function.
    //-----------------------------------------------------------------------------
    void get() const
    {
        wait();
        state_->get_value_assuming_ready();
    }

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    /// Launch policy asyncmeans that the continuation will be queued
    /// regardless if the calling thread is the same as the destination thread.
    /// Policy deferred will only queue it if the calling thread
    /// and the destination thread are different otherwise it
    /// will immediately execute the continuation when the future is ready.
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, std::launch policy, F&& f) const -> future<then_ret_type<F, shared_future<void>>>;
    template<typename F>
    auto then(std::launch policy, F&& f) const -> future<then_ret_type<F, shared_future<void>>>;

    //-----------------------------------------------------------------------------
    /// Attach the continuation func to *this. The behavior is undefined
    /// if *this has no associated shared state (i.e., valid() == false).
    //-----------------------------------------------------------------------------
    template<typename F>
    auto then(thread::id id, F&& f) const -> future<then_ret_type<F, shared_future<void>>>;
    template<typename F>
    auto then(F&& f) const -> future<then_ret_type<F, shared_future<void>>>;
};

//-----------------------------------------------------------------------------
/// IMPLEMENTATIONS
//-----------------------------------------------------------------------------
template<typename T>
auto make_ready_future(T&& value) -> future<T>
{
    promise<T> prom;
    prom.set_value(std::forward<T>(value));
    return prom.get_future();
}

inline auto make_ready_future() -> future<void>
{
    promise<void> prom;
    prom.set_value();
    return prom.get_future();
}

template<class T>
auto make_exceptional_future(std::exception_ptr ex) -> future<T>
{
    promise<T> p;
    p.set_exception(ex);
    return p.get_future();
}

template<class T, class E>
auto make_exceptional_future(E ex) -> future<T>
{
    promise<T> p;
    p.set_exception(std::make_exception_ptr(ex));
    return p.get_future();
}

template<typename T>
auto future<T>::share() -> shared_future<T>
{
    return {std::move(*this)};
}

inline auto future<void>::share() -> shared_future<void>
{
    return {std::move(*this)};
}
namespace detail
{
template<typename... Args, typename T, typename F, typename Tuple>
auto apply_and_forward_as(promise<T>& p, F&& f, Tuple& params) -> std::enable_if_t<!std::is_same<T, void>::value>
{
    p.set_value(utility::apply(
        [&f](std::decay_t<Args>&... args)
        {
            return std::forward<F>(f)(std::forward<Args>(args)...);
        },
        params));
}
template<typename... Args, typename T, typename F, typename Tuple>
auto apply_and_forward_as(promise<T>& p, F&& f, Tuple& params) -> std::enable_if_t<std::is_same<T, void>::value>
{
    utility::apply(
        [&f](std::decay_t<Args>&... args)
        {
            std::forward<F>(f)(std::forward<Args>(args)...);
        },
        params);
    p.set_value();
}

template<typename T>
struct packaged_task
{
    future<T> callable_future;
    task callable;
};

template<typename F, typename... Args>
auto package_future_task(F&& f, Args&&... args) -> packaged_task<async_ret_type<F, Args...>>
{
    using return_type = async_ret_type<F, Args...>;
    auto prom = promise<return_type>();
    auto fut = prom.get_future();

    // clang-format off

    // All this ugliness is to minimize copies and moves
	return {std::move(fut),
                [
                    p = capture(std::move(prom)),
                    f = capture(std::forward<F>(f)),
                    params = capture(std::forward<Args>(args)...)
                ]() mutable {

				try
				{
					detail::apply_and_forward_as<Args...>(
						std::get<0>(p.get()), std::forward<F>(std::get<0>(f.get())), params.get());
				}
				catch(...)
				{
					try
					{
						// store anything thrown in the promise
						std::get<0>(p.get()).set_exception(std::current_exception());
					}
					catch(...)
					{
					} // set_exception() may throw too
				}
			}};
    // clang-format on
}

inline void launch(thread::id id, std::launch policy, task& func)
{
    if(id == caller_id())
    {
        id = this_thread::get_id();
    }
    // Here we are not using invoke/dispatch
    // directly so that we can avoid repackaging again
    // thus avoinding extra moves/copies
    if(policy == std::launch::async)
    {
        // invoke(id, func);
        detail::invoke_packaged_task(id, func);
    }
    else
    {
        //dispatch(id, func);
        if(this_thread::get_id() == id)
        {
            // directly call it
            func();
        }
        else
        {
            detail::invoke_packaged_task(id, func);
        }
    }
}
} // namespace detail

template<typename F, typename... Args>
auto async(thread::id id, std::launch policy, F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>
{
    auto package = detail::package_future_task(std::forward<F>(f), std::forward<Args>(args)...);
    auto& future = package.callable_future;
    auto& task = package.callable;

    detail::launch(id, policy, task);

    return std::move(future);
}

template<typename F, typename... Args>
auto async(thread::id id, F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>
{
    return async(id, std::launch::deferred | std::launch::async, std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto async(std::launch policy, F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>
{
    thread detached_thread(
        []()
        {
            this_thread::register_this_thread();
            this_thread::wait();
            this_thread::unregister_this_thread();
        });
    detached_thread.detach();
    return async(detached_thread.get_id(), policy, std::forward<F>(f), std::forward<Args>(args)...);
}

template<typename F, typename... Args>
auto async(F&& f, Args&&... args) -> future<async_ret_type<F, Args...>>
{
    return tpp::async(std::launch::deferred | std::launch::async, std::forward<F>(f), std::forward<Args>(args)...);
}
//-----------------------------------------------------------------------------
/// future<T>::then() overloads
//-----------------------------------------------------------------------------
template<typename T>
template<typename F>
auto future<T>::then(thread::id id, std::launch policy, F&& f) -> future<then_ret_type<F, future<T>>>
{
    detail::check_state(this->state_);

    // invalidate the state
    auto state = std::move(this->state_);
    auto package = detail::package_future_task(
        [f = std::forward<F>(f), state]() mutable
        {
            future<T> self(state);
            return utility::invoke(f, std::move(self));
        });
    auto& future = package.callable_future;
    auto& task = package.callable;

    state->set_continuation(
        [id, policy, task = std::move(task)]() mutable
        {
            detail::launch(id, policy, task);
        });

    return std::move(future);
}

template<typename T>
template<typename F>
auto future<T>::then(thread::id id, F&& f) -> future<then_ret_type<F, future<T>>>
{
    return then(id, std::launch::async | std::launch::deferred, std::forward<F>(f));
}

template<typename T>
template<typename F>
auto future<T>::then(std::launch policy, F&& f) -> future<then_ret_type<F, future<T>>>
{
    thread detached_thread(
        []()
        {
            this_thread::register_this_thread();
            this_thread::wait();
            this_thread::unregister_this_thread();
        });
    detached_thread.detach();
    return then(detached_thread.get_id(), policy, std::forward<F>(f));
}

template<typename T>
template<typename F>
auto future<T>::then(F&& f) -> future<then_ret_type<F, future<T>>>
{
    return then(std::launch::async | std::launch::deferred, std::forward<F>(f));
}

//-----------------------------------------------------------------------------
/// shared_future<T>::then() overloads
//-----------------------------------------------------------------------------
template<typename T>
template<typename F>
auto shared_future<T>::then(thread::id id, std::launch policy, F&& f) const
    -> future<then_ret_type<F, shared_future<T>>>
{
    detail::check_state(this->state_);

    // do not invalidate the state
    auto state = this->state_;
    auto package = detail::package_future_task(
        [f = std::forward<F>(f), state]() mutable
        {
            shared_future<T> self(state);
            return utility::invoke(f, std::move(self));
        });
    auto& future = package.callable_future;
    auto& task = package.callable;

    state->set_continuation(
        [id, policy, task = std::move(task)]() mutable
        {
            detail::launch(id, policy, task);
        });

    return std::move(future);
}

template<typename T>
template<typename F>
auto shared_future<T>::then(thread::id id, F&& f) const -> future<then_ret_type<F, shared_future<T>>>
{
    return then(id, std::launch::async | std::launch::deferred, std::forward<F>(f));
}

template<typename T>
template<typename F>
auto shared_future<T>::then(std::launch policy, F&& f) const -> future<then_ret_type<F, shared_future<T>>>
{
    thread detached_thread(
        []()
        {
            this_thread::register_this_thread();
            this_thread::wait();
            this_thread::unregister_this_thread();
        });
    detached_thread.detach();
    return then(detached_thread.get_id(), policy, std::forward<F>(f));
}

template<typename T>
template<typename F>
auto shared_future<T>::then(F&& f) const -> future<then_ret_type<F, shared_future<T>>>
{
    return then(std::launch::async | std::launch::deferred, std::forward<F>(f));
}

//-----------------------------------------------------------------------------
/// future<void>::then() overloads
//-----------------------------------------------------------------------------
template<typename F>
auto future<void>::then(thread::id id, std::launch policy, F&& f) -> future<then_ret_type<F, future<void>>>
{
    detail::check_state(this->state_);

    // invalidate the state
    auto state = std::move(this->state_);
    auto package = detail::package_future_task(
        [f = std::forward<F>(f), state]() mutable
        {
            future<void> self(state);
            utility::invoke(f, std::move(self));
        });
    auto& future = package.callable_future;
    auto& task = package.callable;

    state->set_continuation(
        [id, policy, task = std::move(task)]() mutable
        {
            detail::launch(id, policy, task);
        });

    return std::move(future);
}

template<typename F>
auto future<void>::then(thread::id id, F&& f) -> future<then_ret_type<F, future<void>>>
{
    return then(id, std::launch::async | std::launch::deferred, std::forward<F>(f));
}

template<typename F>
auto future<void>::then(std::launch policy, F&& f) -> future<then_ret_type<F, future<void>>>
{
    thread detached_thread(
        []()
        {
            this_thread::register_this_thread();
            this_thread::wait();
            this_thread::unregister_this_thread();
        });
    detached_thread.detach();
    return then(detached_thread.get_id(), policy, std::forward<F>(f));
}

template<typename F>
auto future<void>::then(F&& f) -> future<then_ret_type<F, future<void>>>
{
    return then(std::launch::async | std::launch::deferred, std::forward<F>(f));
}

//-----------------------------------------------------------------------------
/// shared_future<void>::then() overloads
//-----------------------------------------------------------------------------
template<typename F>
auto shared_future<void>::then(thread::id id, std::launch policy, F&& f) const
    -> future<then_ret_type<F, shared_future<void>>>
{
    detail::check_state(this->state_);

    // do not invalidate the state
    auto state = this->state_;
    auto package = detail::package_future_task(
        [f = std::forward<F>(f), state]() mutable
        {
            shared_future<void> self(state);
            utility::invoke(f, std::move(self));
        });
    auto& future = package.callable_future;
    auto& task = package.callable;

    state->set_continuation(
        [id, policy, task = std::move(task)]() mutable
        {
            detail::launch(id, policy, task);
        });

    return std::move(future);
}

template<typename F>
auto shared_future<void>::then(thread::id id, F&& f) const -> future<then_ret_type<F, shared_future<void>>>
{
    return then(id, std::launch::async | std::launch::deferred, std::forward<F>(f));
}

template<typename F>
auto shared_future<void>::then(std::launch policy, F&& f) const -> future<then_ret_type<F, shared_future<void>>>
{
    thread detached_thread(
        []()
        {
            this_thread::register_this_thread();
            this_thread::wait();
            this_thread::unregister_this_thread();
        });
    detached_thread.detach();
    return then(detached_thread.get_id(), policy, std::forward<F>(f));
}

template<typename F>
auto shared_future<void>::then(F&& f) const -> future<then_ret_type<F, shared_future<void>>>
{
    return then(std::launch::async | std::launch::deferred, std::forward<F>(f));
}
} // namespace tpp
