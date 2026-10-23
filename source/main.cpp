#include "ThreadPool.h"
#include <iostream>

int main()
{
    std::mutex iostream_m;
    ThreadPool my_pool{ 4 };

    int job_n = 0;
    for (int i = 0; i < 100; ++i)
    {
        my_pool.push({
          TaskType::Execute, // TaskType
          [&iostream_m, job_n](std::vector<Parameter> const&) // Lambda
          {
            {
              std::lock_guard cout_guard{iostream_m};
              std::cout << "Hi from thread " << std::this_thread::get_id()
                        << " request " << job_n << '\n';
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
          },
          {} // Arguments
            });
        job_n++;
    }
}