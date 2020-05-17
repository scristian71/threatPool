#pragma once

#include <mutex>
#include <queue>
#include <atomic>
#include <chrono>
#include <utility>
#include <stdexcept>
#include <type_traits>
#include <condition_variable>
#include "semaphore.h"
#include <thread>
#include <fstream>
#include <string>

template<typename T>
class blocking_queue
{
public:
    template<typename Q = T>
    typename std::enable_if<std::is_copy_constructible<Q>::value, void>::type
    push(const T& item)
    {
        {
            std::unique_lock lock(m_mutex);
            m_queue.push(item);
        }
        m_ready.notify_one();
    }

    template<typename Q = T>
    typename std::enable_if<std::is_move_constructible<Q>::value, void>::type
    push(T&& item)
    {
        {
            std::unique_lock lock(m_mutex);
            m_queue.emplace(std::forward<T>(item));
        }
        m_ready.notify_one();
    }

    template<typename Q = T>
    typename std::enable_if<std::is_copy_constructible<Q>::value, bool>::type
    try_push(const T& item)
    {
        {
            std::unique_lock lock(m_mutex, std::try_to_lock);
            if(!lock)
                return false;
            m_queue.push(item);
        }
        m_ready.notify_one();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<std::is_move_constructible<Q>::value, bool>::type
    try_push(T&& item)
    {
        {
            std::unique_lock lock(m_mutex, std::try_to_lock);
            if(!lock)
                return false;
            m_queue.emplace(std::forward<T>(item));
        }
        m_ready.notify_one();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_copy_assignable<Q>::value &&
        !std::is_move_assignable<Q>::value, bool>::type
    pop(T& item)
    {
        std::unique_lock lock(m_mutex);
        while(m_queue.empty() && !m_done)
            m_ready.wait(lock);
        if(m_queue.empty())
            return false;
        item = m_queue.front();
        m_queue.pop();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<std::is_move_assignable<Q>::value, bool>::type
    pop(T& item)
    {
        std::unique_lock lock(m_mutex);
        while(m_queue.empty() && !m_done)
            m_ready.wait(lock);
        if(m_queue.empty())
            return false;
        item = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_copy_assignable<Q>::value &&
        !std::is_move_assignable<Q>::value, bool>::type
    try_pop(T& item)
    {
        std::unique_lock lock(m_mutex, std::try_to_lock);
        if(!lock || m_queue.empty())
            return false;
        item = m_queue.front();
        m_queue.pop();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<std::is_move_assignable<Q>::value, bool>::type
    try_pop(T& item)
    {
        std::unique_lock lock(m_mutex, std::try_to_lock);
        if(!lock || m_queue.empty())
            return false;
        item = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }

    void done() noexcept
    {
        {
            std::unique_lock lock(m_mutex);
            m_done = true;
        }
        m_ready.notify_all();
    }

    bool empty() const noexcept
    {
        std::scoped_lock lock(m_mutex);
        return m_queue.empty();
    }

    unsigned int size() const noexcept
    {
        std::scoped_lock lock(m_mutex);
        return m_queue.size();
    }

private:
    std::queue<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_ready;
    bool m_done = false;
};

template<typename T, typename S>
class fixed_blocking_queue
{
public:
    explicit fixed_blocking_queue(unsigned int size = 4)
    : m_size(size), m_pushIndex(0), m_popIndex(0), m_count(0),
    m_data((T*)operator new(size * sizeof(T))),
    m_openSlots(size), m_fullSlots(0)
    {
        if(!size)
            throw std::invalid_argument("Invalid queue size!");
    }

    ~fixed_blocking_queue() noexcept
    {
        while (m_count--)
        {
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
        }
        operator delete(m_data);
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_copy_constructible<Q>::value &&
        std::is_nothrow_copy_constructible<Q>::value, void>::type
    push(const T& item) noexcept
    {
        if(!m_openSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            new (m_data + m_pushIndex) T (item);
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_copy_constructible<Q>::value &&
        !std::is_nothrow_copy_constructible<Q>::value, void>::type
    push(const T& item)
    {
        if(!m_openSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                new (m_data + m_pushIndex) T (item);
            }
            catch (...)
            {
                m_openSlots.post();
                throw;
            }
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_constructible<Q>::value &&
        std::is_nothrow_move_constructible<Q>::value, void>::type
    push(T&& item) noexcept
    {
        if(!m_openSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            new (m_data + m_pushIndex) T (std::move(item));
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_constructible<Q>::value &&
        !std::is_nothrow_move_constructible<Q>::value, void>::type
    push(T&& item)
    {
        if(!m_openSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                new (m_data + m_pushIndex) T (std::move(item));
            }
            catch (...)
            {
                m_openSlots.post();
                throw;
            }
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_copy_constructible<Q>::value &&
        std::is_nothrow_copy_constructible<Q>::value, bool>::type
    try_push(const T& item) noexcept
    {
        auto result = m_openSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            new (m_data + m_pushIndex) T (item);
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_copy_constructible<Q>::value &&
        !std::is_nothrow_copy_constructible<Q>::value, bool>::type
    try_push(const T& item)
    {
        auto result = m_openSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                new (m_data + m_pushIndex) T (item);
            }
            catch (...)
            {
                m_openSlots.post();
                throw;
            }
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_constructible<Q>::value &&
        std::is_nothrow_move_constructible<Q>::value, bool>::type
    try_push(T&& item) noexcept
    {
        auto result = m_openSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            new (m_data + m_pushIndex) T (std::move(item));
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_constructible<Q>::value &&
        !std::is_nothrow_move_constructible<Q>::value, bool>::type
    try_push(T&& item)
    {
        auto result = m_openSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                new (m_data + m_pushIndex) T (std::move(item));
            }
            catch (...)
            {
                m_openSlots.post();
                throw;
            }
            m_pushIndex = ++m_pushIndex % m_size;
            ++m_count;
        }
        m_fullSlots.post();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        !std::is_move_assignable<Q>::value &&
        std::is_nothrow_copy_assignable<Q>::value, void>::type
    pop(T& item) noexcept
    {
        if(!m_fullSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            item = m_data[m_popIndex];
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        !std::is_move_assignable<Q>::value &&
        !std::is_nothrow_copy_assignable<Q>::value, void>::type
    pop(T& item)
    {
        if(!m_fullSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                item = m_data[m_popIndex];
            }
            catch (...)
            {
                m_fullSlots.post();
                throw;
            }
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_assignable<Q>::value &&
        std::is_nothrow_move_assignable<Q>::value, void>::type
    pop(T& item) noexcept
    {
        if(!m_fullSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            item = std::move(m_data[m_popIndex]);
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_assignable<Q>::value &&
        !std::is_nothrow_move_assignable<Q>::value, void>::type
    pop(T& item)
    {
        if (!m_fullSlots.wait())
            return;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                item = std::move(m_data[m_popIndex]);
            }
            catch (...)
            {
                m_fullSlots.post();
                throw;
            }
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
    }

    template<typename Q = T>
    typename std::enable_if<
        !std::is_move_assignable<Q>::value &&
        std::is_nothrow_copy_assignable<Q>::value, bool>::type
    try_pop(T& item) noexcept
    {
        auto result = m_fullSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            item = m_data[m_popIndex];
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        !std::is_move_assignable<Q>::value &&
        !std::is_nothrow_copy_assignable<Q>::value, bool>::type
    try_pop(T& item)
    {
        auto result = m_fullSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                item = m_data[m_popIndex];
            }
            catch (...)
            {
                m_fullSlots.post();
                throw;
            }
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_assignable<Q>::value &&
        std::is_nothrow_move_assignable<Q>::value, bool>::type
    try_pop(T& item) noexcept
    {
        auto result = m_fullSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            item = std::move(m_data[m_popIndex]);
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
        return true;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_move_assignable<Q>::value &&
        !std::is_nothrow_move_assignable<Q>::value, bool>::type
    try_pop(T& item)
    {
        auto result = m_fullSlots.wait_for(std::chrono::seconds(0));
        if(!result)
            return false;
        {
            std::scoped_lock lock(m_cs);
            try
            {
                item = std::move(m_data[m_popIndex]);
            }
            catch (...)
            {
                m_fullSlots.post();
                throw;
            }
            m_data[m_popIndex].~T();
            m_popIndex = ++m_popIndex % m_size;
            --m_count;
        }
        m_openSlots.post();
        return true;
    }

    bool empty() const noexcept
    {
        std::scoped_lock lock(m_cs);
        return m_count == 0;
    }

    bool full() const noexcept
    {
        std::scoped_lock lock(m_cs);
        return m_count == m_size;
    }

    unsigned int size() const noexcept
    {
        std::scoped_lock lock(m_cs);
        return m_count;
    }

    unsigned int capacity() const noexcept
    {
        return m_size;
    }

    void done() noexcept
    {
        m_openSlots.done();
        m_fullSlots.done();
    }

private:
    const unsigned int m_size;
    unsigned int m_pushIndex;
    unsigned int m_popIndex;
    unsigned int m_count;
    T* m_data;

    S m_openSlots;
    S m_fullSlots;
    mutable std::mutex m_cs;
};

template<typename T,
         unsigned long Q_SIZE = 4096ul>
class atomic_blocking_queue_impl
{
public:
    static constexpr unsigned long Q_MASK = Q_SIZE - 1;

    explicit atomic_blocking_queue_impl()
    :
      m_pushIndex(0),
      m_popIndex(0),
      m_pushingIndex(0),
      m_popingIndex(0),
      m_data(
          reinterpret_cast<T*>(
              ::operator new(sizeof(T) * Q_SIZE,
                             std::align_val_t(4096))))
    {
        if(!Q_SIZE)
            throw std::invalid_argument("Invalid queue size!");
    }

    ~atomic_blocking_queue_impl() noexcept
    {
        while (m_popIndex & Q_MASK != m_pushIndex & Q_MASK)
        {
            m_data[m_popIndex & Q_MASK].~T();
            m_popIndex++;
        }
        ::operator delete(m_data);
    }

    template<typename Q = T>
    typename std::enable_if<
    std::is_nothrow_copy_constructible<Q>::value ||
    std::is_nothrow_move_constructible<Q>::value, void>::type
    push(T&& item) noexcept
    {
        const auto expected = m_pushingIndex.fetch_add(1);

        new (m_data + (expected & Q_MASK)) T (std::forward<T>(item));

        while (expected != m_pushIndex)
        {
            std::this_thread::yield();
        }
        m_pushIndex++;
    }

    template<typename Q = T>
    typename std::enable_if<
        std::is_nothrow_copy_assignable<Q>::value ||
        std::is_nothrow_move_assignable<Q>::value, void>::type
    pop(T& item) noexcept
    {
        const auto expected = m_popingIndex.fetch_add(1);

        item = std::move(m_data[expected & Q_MASK]);
        m_data[expected & Q_MASK].~T();

        while (expected != m_popIndex)
        {
            std::this_thread::yield();
        }
        m_popIndex++;
    }

private:
    alignas(64) volatile unsigned int m_pushIndex;
    alignas(64) volatile unsigned int m_popIndex;

    alignas(64) std::atomic_uint m_pushingIndex;
    alignas(64) std::atomic_uint m_popingIndex;

    T* m_data;
};

template<
        typename T,
        typename S,
        unsigned long Q_SIZE = 4096ul,
        template<typename TL, unsigned long Q_SIZEL> typename Q = atomic_blocking_queue_impl>
class atomic_blocking_queue
{
public:
    explicit atomic_blocking_queue()
    :
      m_openSlots(Q_SIZE),
      m_fullSlots(0)
    {
        if(!Q_SIZE)
            throw std::invalid_argument("Invalid queue size!");
    }

    ~atomic_blocking_queue() noexcept  {}

    template<typename W = T>
    typename std::enable_if<
    std::is_nothrow_move_constructible<W>::value ||
    std::is_nothrow_copy_constructible<W>::value, void>::type
    push(T&& item) noexcept
    {
        if (!m_openSlots.wait())
        {
            return;
        }

        queue_impl.push(std::forward<T>(item));

        m_fullSlots.post();
    }

    template<typename W = T>
    typename std::enable_if<
    std::is_nothrow_move_constructible<W>::value ||
    std::is_nothrow_copy_constructible<W>::value, bool>::type
    try_push(T&& item) noexcept
    {
        if(!m_openSlots.wait_for(std::chrono::seconds(0)))
        {
            return false;
        }

        queue_impl.push(std::forward<T>(item));

        m_fullSlots.post();
        return true;
    }

    template<typename W = T>
    typename std::enable_if<
        std::is_nothrow_move_assignable<W>::value ||
        std::is_nothrow_copy_assignable<W>::value, bool>::type
    pop(T& item) noexcept
    {
        if (!m_fullSlots.wait())
        {
            return false;
        }

        queue_impl.pop(item);

        m_openSlots.post();
        return true;
    }

    template<typename W = T>
    typename std::enable_if<
        std::is_nothrow_move_assignable<W>::value ||
        std::is_nothrow_copy_assignable<W>::value, bool>::type
    try_pop(T& item) noexcept
    {
        if(!m_fullSlots.wait_for(std::chrono::seconds(0)))
        {
            return false;
        }

        queue_impl.pop(item);

        m_openSlots.post();
        return true;
    }

    void done() noexcept
    {
        m_done = true;
        m_openSlots.done();
        m_fullSlots.done();
    }

private:

    Q<T, Q_SIZE> queue_impl;

    alignas(64) S m_openSlots;
    alignas(64) S m_fullSlots;

    bool m_done = false;
};

