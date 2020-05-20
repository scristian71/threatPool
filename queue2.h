/*
 * ------------------------------------------------------------------------
 * Lock-free N-producers M-consumers ring-buffer queue.
 * ABA problem safe.
 *
 * This implementation is bit complicated, so possibly it has sense to use
 * classic list-based queues. See:
 * 1. D.Fober, Y.Orlarey, S.Letz, "Lock-Free Techniques for Concurrent
 *    Access to Shared Ojects"
 * 2. M.M.Michael, M.L.Scott, "Simple, Fast and Practical Non-Blocking and
 *    Blocking Concurrent Queue Algorithms"
 * 3. E.Ladan-Mozes, N.Shavit, "An Optimistic Approach to Lock-Free FIFO Queues"
 *
 * See also implementation of N-producers M-consumers FIFO and
 * 1-producer 1-consumer ring-buffer from Tim Blechmann:
 *	http://tim.klingt.org/boost_lockfree/
 *	git://tim.klingt.org/boost_lockfree.git
 * 
 * See See Intel 64 and IA-32 Architectures Software Developer's Manual,
 * Volume 3, Chapter 8.2 Memory Ordering for x86 memory ordering guarantees.
 * ------------------------------------------------------------------------
 */
#include <cstddef>
#include <stdlib.h>
#include <cassert>
#include <cstring>
#include <atomic>
#include <immintrin.h>
#include <limits.h>
#include <new>
#include <algorithm>

static thread_local size_t  __thr_id;

/**
 * @return continous thread IDs starting from 0 as opposed to pthread_self().
 */
inline size_t
thr_id()
{
    return __thr_id;
}

inline void
set_thr_id(size_t id)
{
    __thr_id = id;
}

template<class T,
        unsigned long Q_SIZE = 4096>
class LockFreeQueue {
private:
    static constexpr unsigned long Q_MASK = Q_SIZE - 1;

    struct ThrPos
    {
        unsigned long head, tail;
    };

public:
    LockFreeQueue(size_t n_producers = 1, size_t n_consumers = 2)
        :
        n_producers_(n_producers),
        n_consumers_(n_consumers),
        head_(0),
        tail_(0),
        last_head_(0),
        last_tail_(0)
    {
        auto n = std::max(n_consumers_, n_producers_);
        thr_p_ = reinterpret_cast<ThrPos*>(
                    ::operator new(
                        sizeof(ThrPos) * n,
                        std::align_val_t(4096)));

        // Set per thread tail and head to ULONG_MAX.
        std::memset((void *)thr_p_, 0xFF, sizeof(ThrPos) * n);

        ptr_array_ = reinterpret_cast<T*>(
                        ::operator new(
                            sizeof(T) * Q_SIZE,
                            std::align_val_t(4096)));
    }

    ~LockFreeQueue()
    {
        ::operator delete(ptr_array_);
        ::operator delete(thr_p_);
    }

    ThrPos&
    thr_pos() const
    {
        assert(thr_id() < std::max(n_consumers_, n_producers_));
        return thr_p_[thr_id()];
    }

    void
    push(T&& t)
    {
        ThrPos& tp = thr_pos();
        /*
         * Request next place to push.
         *
         * Second assignemnt is atomic only for head shift, so there is
         * a time window in which thr_p_[tid].head = ULONG_MAX, and
         * head could be shifted significantly by other threads,
         * so pop() will set last_head_ to head.
         * After that thr_p_[tid].head is setted to old head value
         * (which is stored in local CPU register) and written by @ptr.
         *
         * First assignment guaranties that pop() sees values for
         * head and thr_p_[tid].head not greater that they will be
         * after the second assignment with head shift.
         *
         * Loads and stores are not reordered with locked instructions,
         * so we don't need a memory barrier here.
         */
        tp.head = head_;
        tp.head = head_.fetch_add(1);

        /*
         * We do not know when a consumer uses the pop()'ed pointer,
         * so we can not overwrite it and have to wait the lowest tail.
         */
        while (tp.head >= last_tail_ + Q_SIZE)
        {
            auto min = tail_.load();

            // Update the last_tail_.
            for (size_t i = 0; i < n_consumers_; ++i)
            {
                const auto tmp_t = thr_p_[i].tail;

                // Force compiler to use tmp_h exactly once.
                //asm volatile("" ::: "memory");

                if (tmp_t < min)
                    min = tmp_t;
            }
            last_tail_ = min;

            if (tp.head < last_tail_ + Q_SIZE)
                break;
            //_mm_pause();
            std::this_thread::yield();
        }

        new (ptr_array_ + (tp.head & Q_MASK)) T (std::forward<T>(t));

        // Allow consumers eat the item.
        tp.head = ULONG_MAX;
    }

    void
    pop(T& t)
    {
        ThrPos& tp = thr_pos();
        /*
         * Request next place from which to pop.
         * See comments for push().
         *
         * Loads and stores are not reordered with locked instructions,
         * so we don't need a memory barrier here.
         */
        tp.tail = tail_.load();
        tp.tail = tail_.fetch_add(1);

        /*
         * tid'th place in ptr_array_ is reserved by the thread -
         * this place shall never be rewritten by push() and
         * last_tail_ at push() is a guarantee.
         * last_head_ guaraties that no any consumer eats the item
         * before producer reserved the position writes to it.
         */
        while (tp.tail >= last_head_)
        {
            auto min = head_.load();

            // Update the last_head_.
            for (size_t i = 0; i < n_producers_; ++i)
            {
                auto tmp_h = thr_p_[i].head;

                // Force compiler to use tmp_h exactly once.
                //asm volatile("" ::: "memory");

                if (tmp_h < min)
                    min = tmp_h;
            }
            last_head_ = min;

            if (tp.tail < last_head_)
                break;
            //_mm_pause();
            std::this_thread::yield();
        }

        t.swap(ptr_array_[tp.tail & Q_MASK]);
        ptr_array_[tp.tail & Q_MASK].~T();

        // Allow producers rewrite the slot.
        tp.tail = ULONG_MAX;
    }

private:
    /*
     * The most hot members are cacheline aligned to avoid
     * False Sharing.
     */

    const size_t n_producers_, n_consumers_;
    // currently free position (next to insert)
    std::atomic<unsigned long>  head_ alignas (64);
    // current tail, next to pop
    std::atomic<unsigned long>  tail_ alignas (64);
    // last not-processed producer's pointer
    volatile unsigned long  last_head_ alignas (64);
    // last not-processed consumer's pointer
    volatile unsigned long  last_tail_ alignas (64);
    ThrPos* thr_p_;
    T*      ptr_array_;
};

