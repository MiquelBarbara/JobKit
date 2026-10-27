#include <JobKit/JobGroup.h>
#include <JobKit/ThreadPool.h>

#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

int main()
{
    try
    {
        // Data, then pool, then group: accepted batches drain before data teardown
        // even if dispatch accepts only part of the work before throwing.
        std::vector<std::uint64_t> squares(256);
        JobKit::ThreadPool pool(2);
        JobKit::JobGroup group;
        pool.dispatch(squares.size(), 32,
            [&squares](std::size_t begin, std::size_t end)
            {
                for (auto i = begin; i < end; ++i)
                {
                    squares[i] = (i + 1) * (i + 1);
                }
            }, &group);
        group.wait();

        // A move-only capture and result; only the controlling thread waits.
        auto sum = pool.submit([values = std::make_unique<std::vector<std::uint64_t>>(std::move(squares))]
        {
            return std::make_unique<std::uint64_t>(
                std::accumulate(values->begin(), values->end(), std::uint64_t{0}));
        });
        std::cout << "Sum of squares 1..256: " << *sum.get() << '\n';
        pool.wait();
    }
    catch (const std::exception& error)
    {
        std::cerr << "JobKit example: " << error.what() << '\n';
        return 1;
    }
}
