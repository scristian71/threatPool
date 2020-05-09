/*
 * fast_semaphore designed by Joe Seigh, implemented by Chris Thomasson
 *
 * https://www.haiku-os.org/legacy-docs/benewsletter/Issue1-26.html
 */
#pragma once

#include <mutex>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <limits.h>

class semaphore
{
public:
    explicit semaphore(unsigned int count = 0) noexcept
    : m_count(count) {}

    void post() noexcept
    {
        {
            std::unique_lock lock(m_mutex);

            ++m_count;
        }

        m_cv.notify_one();
    }

    [[nodiscard]] bool wait() noexcept
    {
        std::unique_lock lock(m_mutex);

        m_cv.wait(lock, [this]() { return m_count != 0 || m_done; });

        auto wait_result = m_count != 0;
        if (wait_result)
            --m_count;
        return wait_result;
    }

    template<typename T>
    [[nodiscard]] bool wait_for(T&& t) noexcept
    {
        std::unique_lock lock(m_mutex);

        m_cv.wait_for(lock, t, [this]() { return m_count != 0 || m_done; });

        auto wait_result = m_count != 0;
        if (wait_result)
            --m_count;
        return wait_result;
    }

    template<typename T>
    bool wait_until(T&& t) noexcept
    {
        std::unique_lock lock(m_mutex);

        m_cv.wait_until(lock, t, [this]() { return m_count != 0 || m_done; });

        auto wait_result = m_count != 0;
        if (wait_result)
            --m_count;
        return wait_result;
    }

    void done() noexcept
    {
        {
            std::unique_lock lock(m_mutex);
            m_done = true;
        }
        m_cv.notify_all();
    }
private:
    unsigned int m_count;

    std::mutex m_mutex;
    std::condition_variable m_cv;

    bool m_done = false;
};

class fast_semaphore
{
public:
    explicit fast_semaphore(int init_count = 0) noexcept
    : m_count(init_count), m_semaphore(0) {}

    void post() noexcept
    {
        int oldCount = m_count.fetch_add(1, std::memory_order_release);

        if (oldCount < 0)
        {
            m_semaphore.post();
        }
    }

    bool waitWithPartialSpinning()
    {
        int oldCount;
        // Is there a better way to set the initial spin count?
        // If we lower it to 1000, testBenaphore becomes 15x slower on my Core i7-5930K Windows PC,
        // as threads start hitting the kernel semaphore.
        int spin = 10000;
        while (spin--)
        {
            oldCount = m_count.load(std::memory_order_relaxed);
            if ((oldCount > 0) && m_count.compare_exchange_strong(oldCount, oldCount - 1, std::memory_order_acquire))
                return true;
            std::atomic_signal_fence(std::memory_order_acquire);     // Prevent the compiler from collapsing the loop.
        }
        oldCount = m_count.fetch_sub(1, std::memory_order_acquire);
        if (oldCount <= 0)
        {
            return m_semaphore.wait();
        }
        return true;
    }

    bool tryWait()
    {
        int oldCount = m_count.load(std::memory_order_relaxed);
        return (oldCount > 0 && m_count.compare_exchange_strong(oldCount, oldCount - 1, std::memory_order_acquire));
    }

    [[nodiscard]] bool wait() noexcept
    {
        if (!tryWait())
            return waitWithPartialSpinning();
        else
            return true;
    }

    template<typename T>
    [[nodiscard]] bool wait_for(T&& t) noexcept
    {
        return tryWait();
    }

    void done() noexcept
    {
        m_semaphore.done();
    }

private:
    std::atomic_int m_count;
    semaphore m_semaphore;
};
