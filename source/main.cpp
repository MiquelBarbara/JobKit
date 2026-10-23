#include "ThreadPool.h"
#include <algorithm>
#include <array>
#include <iostream>
#include <chrono>
#include <utility>

// Set a switch to 0 to disable that test, then rebuild.
// These can also be overridden in the project's preprocessor definitions.
#ifndef JOBKIT_TEST_PUSH
#define JOBKIT_TEST_PUSH 1
#endif

#ifndef JOBKIT_TEST_DISPATCH
#define JOBKIT_TEST_DISPATCH 1
#endif

#if JOBKIT_TEST_PUSH
static bool testPush(ThreadPool& pool, std::mutex& outputMutex)
{
    std::array<int, 100> visits{};

    for (std::size_t job = 0; job < visits.size(); ++job)
    {
        pool.push({
            TaskType::Execute,
            [&outputMutex, &visits, job](std::vector<Parameter> const&) {
                {
                    std::lock_guard lock(outputMutex);
                    ++visits[job];
                    std::cout << "Hi from thread " << std::this_thread::get_id()
                              << " request " << job << '\n';
                }
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            },
            {}
        });
    }

    pool.wait();
    const bool passed = std::all_of(visits.begin(), visits.end(),
        [](int count) { return count == 1; });
    std::cout << "[" << (passed ? "PASS" : "FAIL") << "] push: 100 tasks exactly once\n";
    return passed;
}
#endif

#if JOBKIT_TEST_DISPATCH
using Range = std::pair<std::size_t, std::size_t>;

static bool testDispatchCase(ThreadPool& pool, std::mutex& outputMutex,
    std::size_t elementCount, std::size_t batchSize,
    const std::vector<Range>& expectedRanges)
{
    std::vector<int> visits(elementCount, 0);
    std::vector<Range> actualRanges;
    bool validRanges = true;

    pool.dispatch(elementCount, batchSize,
        [&](std::size_t begin, std::size_t end) {
            // Protect the test's shared records and console output.
            std::lock_guard lock(outputMutex);
            actualRanges.emplace_back(begin, end);
            std::cout << "dispatch(" << elementCount << ", " << batchSize
                      << "): [" << begin << ", " << end << ") on thread "
                      << std::this_thread::get_id() << '\n';

            if (begin >= end || end > elementCount)
            {
                validRanges = false;
                return;
            }

            for (std::size_t i = begin; i < end; ++i)
            {
                ++visits[i];
            }
        });

    // All referenced test data stays alive until its callbacks have finished.
    pool.wait();
    std::sort(actualRanges.begin(), actualRanges.end());
    const bool passed = validRanges && actualRanges == expectedRanges
        && std::all_of(visits.begin(), visits.end(),
            [](int count) { return count == 1; });

    std::cout << "[" << (passed ? "PASS" : "FAIL") << "] dispatch("
              << elementCount << ", " << batchSize << ")\n";
    return passed;
}

static bool testDispatch(ThreadPool& pool, std::mutex& outputMutex)
{
    bool passed = true;
    passed = testDispatchCase(pool, outputMutex, 10, 4,
        {{0, 4}, {4, 8}, {8, 10}}) && passed;
    passed = testDispatchCase(pool, outputMutex, 0, 4, {}) && passed;
    passed = testDispatchCase(pool, outputMutex, 8, 4,
        {{0, 4}, {4, 8}}) && passed;
    passed = testDispatchCase(pool, outputMutex, 3, 4, {{0, 3}}) && passed;
    passed = testDispatchCase(pool, outputMutex, 1, 1, {{0, 1}}) && passed;
    return passed;
}
#endif

int main()
{
    std::mutex outputMutex;
    ThreadPool pool{4};
    bool passed = true;

#if JOBKIT_TEST_PUSH
    passed = testPush(pool, outputMutex) && passed;
#endif

#if JOBKIT_TEST_DISPATCH
    passed = testDispatch(pool, outputMutex) && passed;
#endif

    std::cout << "All enabled tests finished: " << (passed ? "PASS" : "FAIL") << '\n';
    return passed ? 0 : 1;
}
