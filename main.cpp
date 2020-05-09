#include <iostream>
//#include <chrono>

#include "threat_pool.h"

std::atomic<int> nr_primes = 0;

void isPrime(unsigned int nr)
{
    bool result = true;
    for (unsigned int i = 2; i <= nr / 2; i++)
    {
        if (nr % i == 0)
        {
            result = false;
            break;
        }
    }
    if (result)
    {
        nr_primes++;
    }
//    return result;
}
int main(int argc, char* argv[])
{
    auto start = std::chrono::steady_clock::now();

    unsigned int maxnr = 100000;
    unsigned int th_count = 2;
    unsigned int q_count = 2;
    if (argc == 4)
    {
        maxnr = atoi(argv[1]);
        th_count = atoi(argv[2]);
        q_count = atoi(argv[3]);
    }
    {
        thread_pool<> tp(th_count, q_count);
        for (unsigned int j = 0; j < maxnr; j++)
            for (unsigned int i = 3; i < 100; i++,i++)
            {
                tp.enqueue_work(isPrime, i);
            }
        std::cout << "Enqueue ended. Stopping pool..." << std::endl;
    }
    std::cout<< "First " << maxnr << " primes: " << nr_primes << std::endl;
    auto stop = std::chrono::steady_clock::now();
    std::cout << "Duration: " << std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count() << "ms." << std::endl;
    return 0;
}
