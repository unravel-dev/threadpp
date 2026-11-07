#pragma once
#include "future.hpp"
#include <algorithm>
#include <type_traits>

namespace tpp
{

template<typename Sequence>
struct when_any_result
{
    size_t index = size_t(-1);
    Sequence futures{};
};
//-----------------------------------------------------------------------------
/// Create a future object that becomes ready when all of the
/// input futures and shared_futures become ready. The behavior
/// is undefined if any input future or shared_future is invalid.
//-----------------------------------------------------------------------------
template<typename... Futures>
auto when_all(Futures&&... futures) -> future<std::tuple<std::decay_t<Futures>...>>;

//-----------------------------------------------------------------------------
/// Create a future object that becomes ready when all of the
/// input futures and shared_futures become ready. The behavior
/// is undefined if any input future or shared_future is invalid.
//-----------------------------------------------------------------------------
template<class InputIt>
auto when_all(InputIt first, InputIt last) -> future<std::vector<typename std::iterator_traits<InputIt>::value_type>>;

//-----------------------------------------------------------------------------
/// Create a future object that becomes ready when at least one
/// of the input futures and shared_futures become ready.
/// The behavior is undefined if any input future or shared_future is invalid.
//-----------------------------------------------------------------------------
template<typename... Futures>
auto when_any(Futures&&... futures) -> future<when_any_result<std::tuple<std::decay_t<Futures>...>>>;

//-----------------------------------------------------------------------------
/// Create a future object that becomes ready when at least one
/// of the input futures and shared_futures become ready.
/// The behavior is undefined if any input future or shared_future is invalid.
//-----------------------------------------------------------------------------
template<typename InputIt>
auto when_any(InputIt first, InputIt last)
    -> future<when_any_result<std::vector<typename std::iterator_traits<InputIt>::value_type>>>;

//-----------------------------------------------------------------------------
/// A facility to access a tuple's elements by runtime index
//-----------------------------------------------------------------------------
template<typename Tuple, typename F>
void visit_at(Tuple&& tup, size_t idx, F&& f);

//-----------------------------------------------------------------------------
/// IMPLEMENTATION
//-----------------------------------------------------------------------------
namespace detail
{

template<size_t I, typename Context>
void when_any_inner_helper(Context context)
{
    using ith_future_type = std::decay_t<decltype(std::get<I>(context->result.futures))>;
    auto id = this_thread::get_id();

    std::get<I>(context->result.futures)
        .then(id,
              [context](ith_future_type f)
              {
                  std::lock_guard<std::mutex> lock(context->mutex);
                  if(!context->ready)
                  {
                      context->ready = true;
                      context->result.index = I;
                      std::get<I>(context->result.futures) = std::move(f);
                      if(context->processed == context->total && !context->result_moved)
                      {
                          context->recover_input_futures();
                          context->p.set_value(std::move(context->result));
                          context->result_moved = true;
                      }
                  }
              });
}

template<size_t I, size_t S>
struct when_any_helper_struct
{
    template<typename Context, typename... Futures>
    static void apply(const Context& context, std::tuple<Futures...>& t)
    {
        when_any_inner_helper<I>(context);
        ++context->processed;
        when_any_helper_struct<I + 1, S>::apply(context, t);
    }

    template<typename... Futures, typename... States>
    static void recover(std::tuple<Futures...>& t, std::tuple<States...>& s)
    {
        static_assert(sizeof...(Futures) == sizeof...(States), "Sizes mismatch");
        auto& f = std::get<I>(t);
        if(!f.valid())
        {
            f._internal_set_state(std::get<I>(s));
        }
        when_any_helper_struct<I + 1, S>::recover(t, s);
    }
};

template<size_t S>
struct when_any_helper_struct<S, S>
{
    template<typename Context, typename... Futures>
    static void apply(const Context& /*unused*/, std::tuple<Futures...>& /*unused*/)
    {
    }

    template<typename... Futures, typename... States>
    static void recover(std::tuple<Futures...>& /*unused*/, std::tuple<States...>& /*unused*/)
    {
    }
};

template<size_t I, typename Context>
void fill_result_helper(const Context& /*unused*/)
{
}

template<typename T, std::enable_if_t<std::is_copy_constructible<T>::value>* = nullptr>
auto copy_or_move(T&& v) -> decltype(auto)
{
    return std::forward<T>(v);
}

template<typename T, std::enable_if_t<!std::is_copy_constructible<T>::value>* = nullptr>
auto copy_or_move(T&& v) -> decltype(auto)
{
    return std::forward<T>(v);
}
template<typename T, std::enable_if_t<!std::is_copy_constructible<T>::value>* = nullptr>
decltype(auto) copy_or_move(T& v)
{
    return std::move(v);
}

template<size_t I, typename Context, typename FirstFuture, typename... Futures>
void fill_result_helper(const Context& context, FirstFuture&& f, Futures&&... fs)
{
    std::get<I>(context->result.futures) = copy_or_move(std::forward<FirstFuture>(f));
    std::get<I>(context->states) = std::get<I>(context->result.futures)._internal_get_state();

    fill_result_helper<I + 1>(context, std::forward<Futures>(fs)...);
}

template<size_t I, typename Context, typename Future>
void when_inner_helper(Context context, Future&& f)
{
    std::get<I>(context->result) = copy_or_move(std::forward<Future>(f));
    context->result_state->add_antecedent(std::get<I>(context->result)._internal_get_state());
    auto id = this_thread::get_id();

    std::get<I>(context->result)
        .then(id,
              [context](auto f)
              {
                  std::lock_guard<std::mutex> lock(context->mutex);
                  ++context->ready_futures;
                  std::get<I>(context->result) = std::move(f);
                  if(context->ready_futures == context->total_futures)
                  {
                      context->p.set_value(std::move(context->result));
                  }
              });
}

template<size_t I, typename Context>
void apply_helper(const Context& /* ctx*/)
{
}

template<size_t I, typename Context, typename FirstFuture, typename... Futures>
void apply_helper(const Context& context, FirstFuture&& f, Futures&&... fs)
{
    detail::when_inner_helper<I>(context, std::forward<FirstFuture>(f));
    apply_helper<I + 1>(context, std::forward<Futures>(fs)...);
}

template<size_t I>
struct visit_impl
{
    template<typename T, typename F>
    static void visit(T&& tup, size_t idx, F&& fun)
    {
        if(idx == I - 1)
        {
            fun(std::get<I - 1>(tup));
        }
        else
        {
            visit_impl<I - 1>::visit(tup, idx, std::forward<F>(fun));
        }
    }
};

template<>
struct visit_impl<0>
{
    template<typename T, typename F>
    static void visit(T&& /*unused*/, size_t /*unused*/, F&& /*unused*/)
    {
        throw std::runtime_error("bad field index");
    }
};
} // namespace detail

template<typename InputIt>
auto when_all(InputIt first, InputIt last) -> future<std::vector<typename std::iterator_traits<InputIt>::value_type>>
{
    using result_inner_type = std::vector<typename std::iterator_traits<InputIt>::value_type>;
    if(first == last)
    {
        return make_ready_future(result_inner_type{});
    }
    struct context
    {
        size_t total_futures = 0;
        size_t ready_futures = 0;
        result_inner_type result;
        std::mutex mutex;
        promise<result_inner_type> p;
    };

    auto shared_context = std::make_shared<context>();
    auto result_future = shared_context->p.get_future();
    shared_context->total_futures = std::distance(first, last);
    shared_context->result.reserve(shared_context->total_futures);
    size_t index = 0;
    auto id = this_thread::get_id();

    for(; first != last; ++first, ++index)
    {
        shared_context->result.emplace_back(copy_or_move(*first));
        detail::link_to_source(result_future, shared_context->result[index]._internal_get_state());
        shared_context->result[index].then(id,
                                           [shared_context, index](auto f)
                                           {
                                               std::lock_guard<std::mutex> lock(shared_context->mutex);
                                               shared_context->result[index] = std::move(f);
                                               ++shared_context->ready_futures;
                                               if(shared_context->ready_futures == shared_context->total_futures)
                                               {
                                                   shared_context->p.set_value(std::move(shared_context->result));
                                               }
                                           });
    }

    return result_future;
}

inline auto when_all() -> future<std::tuple<>>
{
    return make_ready_future(std::tuple<>{});
}

template<typename... Futures>
auto when_all(Futures&&... futures) -> future<std::tuple<std::decay_t<Futures>...>>
{
    using result_inner_type = std::tuple<std::decay_t<Futures>...>;
    struct context
    {
        size_t total_futures;
        size_t ready_futures = 0;
        result_inner_type result;
        promise<result_inner_type> p;
        // p's state, which waits on the inputs
        std::shared_ptr<detail::state_base> result_state;
        std::mutex mutex;
    };
    auto shared_context = std::make_shared<context>();
    shared_context->total_futures = sizeof...(futures);
    auto result_future = shared_context->p.get_future();
    shared_context->result_state = result_future._internal_get_state();
    detail::apply_helper<0>(shared_context, std::forward<Futures>(futures)...);
    return result_future;
}

template<typename InputIt>
auto when_any(InputIt first, InputIt last)
    -> future<when_any_result<std::vector<typename std::iterator_traits<InputIt>::value_type>>>
{
    using value_type = typename std::iterator_traits<InputIt>::value_type;
    using future_state_inner_type = std::vector<typename value_type::state_type>;
    using result_inner_type = std::vector<value_type>;
    using future_inner_type = when_any_result<result_inner_type>;

    if(first == last)
    {
        return make_ready_future(future_inner_type{});
    }

    struct context
    {
        size_t total = 0;
        std::atomic<size_t> processed;
        future_inner_type result;
        future_state_inner_type states;
        promise<future_inner_type> p;
        bool ready = false;
        bool promise_set = false;
        std::mutex mutex;

        void recover_input_futures()
        {
            for(size_t i = 0; i < result.futures.size(); ++i)
            {
                auto& f = result.futures[i];
                auto state = states[i];
                if(!f.valid())
                {
                    f._internal_set_state(state);
                }
            }
        }
    };

    auto shared_context = std::make_shared<context>();
    auto result_future = shared_context->p.get_future();
    shared_context->processed = 0;
    shared_context->total = std::distance(first, last);
    shared_context->result.futures.reserve(shared_context->total);
    shared_context->states.reserve(shared_context->total);

    size_t index = 0;

    auto id = this_thread::get_id();
    for(; first != last; ++first, ++index)
    {
        shared_context->result.futures.emplace_back(copy_or_move(*first));
        shared_context->states.emplace_back(shared_context->result.futures[index]._internal_get_state());
        detail::link_to_source(result_future, shared_context->states.back());
        shared_context->result.futures[index].then(
            id,
            [shared_context, index](auto f)
            {
                std::lock_guard<std::mutex> lock(shared_context->mutex);
                if(!shared_context->ready)
                {
                    shared_context->result.index = index;
                    shared_context->ready = true;
                    shared_context->result.futures[index] = std::move(f);
                    if(shared_context->processed == shared_context->total && !shared_context->promise_set)
                    {
                        shared_context->recover_input_futures();
                        shared_context->p.set_value(std::move(shared_context->result));
                        shared_context->promise_set = true;
                    }
                }
            });
        ++shared_context->processed;
    }

    {
        std::lock_guard<std::mutex> lock(shared_context->mutex);
        if(shared_context->ready && !shared_context->promise_set)
        {
            shared_context->recover_input_futures();
            shared_context->p.set_value(std::move(shared_context->result));
            shared_context->promise_set = true;
        }
    }

    return result_future;
}
inline auto when_any() -> future<when_any_result<std::tuple<>>>
{
    using result_inner_type = std::tuple<>;
    using future_inner_type = when_any_result<result_inner_type>;
    return make_ready_future(future_inner_type{});
}

template<typename... Futures>
auto when_any(Futures&&... futures) -> future<when_any_result<std::tuple<std::decay_t<Futures>...>>>
{
    using result_inner_type = std::tuple<std::decay_t<Futures>...>;
    using future_state_inner_type = std::tuple<typename std::decay_t<Futures>::state_type...>;
    using future_inner_type = when_any_result<result_inner_type>;
    constexpr static const auto sz_futures = sizeof...(futures);
    struct context
    {
        bool ready = false;
        bool result_moved = false;
        size_t total = 0;
        std::atomic<size_t> processed;
        future_inner_type result;
        future_state_inner_type states;
        promise<future_inner_type> p;
        std::mutex mutex;

        void recover_input_futures()
        {
            detail::when_any_helper_struct<0, sz_futures>::recover(result.futures, states);
        }
    };

    auto shared_context = std::make_shared<context>();
    shared_context->processed = 0;
    shared_context->total = sz_futures;

    auto result_future = shared_context->p.get_future();
    detail::fill_result_helper<0>(shared_context, std::forward<Futures>(futures)...);
    utility::apply(
        [&result_future](const auto&... states)
        {
            (detail::link_to_source(result_future, states), ...);
        },
        shared_context->states);
    detail::when_any_helper_struct<0, sz_futures>::apply(shared_context, shared_context->result.futures);
    {
        std::lock_guard<std::mutex> lock(shared_context->mutex);
        if(shared_context->ready && !shared_context->result_moved)
        {
            shared_context->recover_input_futures();
            shared_context->p.set_value(std::move(shared_context->result));
            shared_context->result_moved = true;
        }
    }
    return result_future;
}

template<typename Tuple, typename F>
void visit_at(Tuple&& tup, size_t idx, F&& f)
{
    detail::visit_impl<std::tuple_size<std::decay_t<Tuple>>::value>::visit(tup, idx, std::forward<F>(f));
}

} // namespace tpp
