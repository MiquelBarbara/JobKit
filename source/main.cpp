#include "ThreadPool.h"
#include "JobGraph.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <future>
#include <iostream>
#include <chrono>
#include <stdexcept>
#include <string_view>
#include <utility>

// Set a switch to 0 to disable that test, then rebuild.
// These can also be overridden in the project's preprocessor definitions.
#ifndef JOBKIT_TEST_PUSH
#define JOBKIT_TEST_PUSH 1
#endif

#ifndef JOBKIT_TEST_DISPATCH
#define JOBKIT_TEST_DISPATCH 1
#endif

#ifndef JOBKIT_TEST_EXCEPTIONS
#define JOBKIT_TEST_EXCEPTIONS 1
#endif

#ifndef JOBKIT_TEST_GROUPS
#define JOBKIT_TEST_GROUPS 1
#endif

#ifndef JOBKIT_TEST_INDEPENDENT_GROUPS
#define JOBKIT_TEST_INDEPENDENT_GROUPS 1
#endif

#ifndef JOBKIT_TEST_SHUTDOWN
#define JOBKIT_TEST_SHUTDOWN 1
#endif

#ifndef JOBKIT_TEST_GRAPH_VALIDATION
#define JOBKIT_TEST_GRAPH_VALIDATION 1
#endif

#if JOBKIT_TEST_PUSH
static bool testPush(ThreadPool& pool, std::mutex& outputMutex)
{
    std::array<int, 100> visits{};

    for (std::size_t job = 0; job < visits.size(); ++job)
    {
        pool.push({
            // Each task owns its job index; referenced records remain alive through wait().
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

    // Completion synchronizes worker writes with the checks below.
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
    // Batch execution order is unspecified, so compare ranges in sorted order.
    std::sort(actualRanges.begin(), actualRanges.end());
    // Matching ranges checks batching; visit counts detect skipped/duplicate elements.
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
    // Repeated dispatch/wait calls also exercise reuse of the same pool.
    // Evaluate each test first so an earlier failure does not skip later cases.
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

#if JOBKIT_TEST_EXCEPTIONS
static bool testExceptions(ThreadPool& pool)
{
    bool otherTaskFinished = false;
    pool.push({
        [](std::vector<Parameter> const&) {
            throw std::logic_error("Expected worker failure");
        },
        {}
    });
    pool.push({
        [&otherTaskFinished](std::vector<Parameter> const&) {
            otherTaskFinished = true;
        },
        {}
    });

    // Catch on the calling thread: wait() must preserve the original type/message
    // and finish the other task before reporting the worker's failure.
    bool originalExceptionReported = false;
    try
    {
        pool.wait();
    }
    catch (const std::logic_error& error)
    {
        originalExceptionReported = std::string_view(error.what()) == "Expected worker failure";
        std::cout << "Caught expected worker exception: " << error.what() << '\n';
    }
    catch (...)
    {
        std::cout << "Caught an unexpected exception type\n";
    }

    const bool failureReported = originalExceptionReported && otherTaskFinished;
    std::cout << "[" << (failureReported ? "PASS" : "FAIL")
              << "] exception: original error reported after all work finishes\n";

    // No new work or failures: the previous exception must not be reported twice.
    bool exceptionCleared = true;
    try
    {
        pool.wait();
    }
    catch (...)
    {
        exceptionCleared = false;
    }
    std::cout << "[" << (exceptionCleared ? "PASS" : "FAIL")
              << "] exception: second wait does not repeat the error\n";

    // Keep captured locals alive through wait(), including its exception path.
    bool reusedTaskFinished = false;
    pool.push({
        [&reusedTaskFinished](std::vector<Parameter> const&) {
            reusedTaskFinished = true;
        },
        {}
    });
    bool reuseWaitSucceeded = true;
    try
    {
        pool.wait();
    }
    catch (...)
    {
        reuseWaitSucceeded = false;
    }
    const bool reusable = reuseWaitSucceeded && reusedTaskFinished;
    std::cout << "[" << (reusable ? "PASS" : "FAIL")
              << "] exception: pool accepts and completes new work\n";

    return failureReported && exceptionCleared && reusable;
}
#endif

#if JOBKIT_TEST_GROUPS
static bool testGroups(ThreadPool& pool, std::mutex& outputMutex)
{
    std::array<int, 12> visits{};
    JobGroup group;

    for (std::size_t job = 0; job < visits.size(); ++job)
    {
        pool.push({
            [&outputMutex, &visits, job](std::vector<Parameter> const&) {
                std::lock_guard lock(outputMutex);
                ++visits[job];
                std::cout << "Grouped task " << job << " on thread "
                          << std::this_thread::get_id() << '\n';
            },
            {},
            &group // Third field associates this task with the group.
        });
    }

    // Wait on the group itself, then check results BEFORE any pool-wide wait.
    // The group and captured locals remain alive until all their work finishes.
    group.wait();
    bool passed = false;
    {
        // Protect test records even if a faulty group wait returns too early.
        std::lock_guard lock(outputMutex);
        passed = std::all_of(visits.begin(), visits.end(),
            [](int count) { return count == 1; });
    }

    // Drain remaining pool bookkeeping before returning and destroying test data.
    pool.wait();
    std::cout << "[" << (passed ? "PASS" : "FAIL")
              << "] group: wait completes all 12 tasks exactly once\n";
    return passed;
}
#endif

#if JOBKIT_TEST_INDEPENDENT_GROUPS
static bool testIndependentGroups(ThreadPool& pool, std::mutex& outputMutex)
{
    // This test needs at least two pool workers: B blocks one while A runs.
    JobGroup groupA;
    JobGroup groupB;
    std::array<int, 10> visits{};
    std::atomic<bool> bFinished{false};
    std::promise<void> bStarted;
    auto bStartedFuture = bStarted.get_future();
    std::promise<void> releaseB;
    auto bGate = releaseB.get_future().share();

    pool.push({
        [&bStarted, &bFinished, bGate](std::vector<Parameter> const&) {
            bStarted.set_value();
            bGate.wait(); // Only the calling thread can release this task.
            bFinished.store(true);
        },
        {},
        &groupB
    });
    const bool bIsRunning = bStartedFuture.wait_for(std::chrono::seconds{5})
        == std::future_status::ready;

    // Exercise grouped dispatch, including a final partial batch [8, 10).
    pool.dispatch(visits.size(), 4,
        [&visits, &outputMutex](std::size_t begin, std::size_t end) {
            std::lock_guard lock(outputMutex);
            for (std::size_t i = begin; i < end; ++i)
            {
                ++visits[i];
            }
        },
        &groupA);

    // Wait outside the pool. A bounded observation lets us release B even if
    // a faulty A.wait() also waits for B, instead of hanging the main thread.
    auto aWaiter = std::async(std::launch::async, [&groupA] { groupA.wait(); });
    const bool aReturned = aWaiter.wait_for(std::chrono::seconds{5})
        == std::future_status::ready;
    bool aCompletedExactlyOnce = false;
    {
        std::lock_guard lock(outputMutex);
        aCompletedExactlyOnce = std::all_of(visits.begin(), visits.end(),
            [](int count) { return count == 1; });
    }
    const bool independent = bIsRunning && aReturned && aCompletedExactlyOnce
        && !bFinished.load();

    // Always release B before cleanup, including when the independence check fails.
    releaseB.set_value();
    bool aWaitSucceeded = true;
    try
    {
        aWaiter.get();
    }
    catch (...)
    {
        aWaitSucceeded = false;
    }
    groupB.wait();
    pool.wait(); // Both groups are released; captured state can now be destroyed.

    const bool passed = independent && aWaitSucceeded;
    std::cout << "[" << (passed ? "PASS" : "FAIL")
              << "] groups: A dispatch finishes while B remains blocked\n";
    const bool bCompleted = bFinished.load();
    std::cout << "[" << (bCompleted ? "PASS" : "FAIL")
              << "] groups: B finishes after its signal is released\n";
    return passed && bCompleted;
}
#endif

#if JOBKIT_TEST_SHUTDOWN
static bool testShutdown()
{
    // Test data and the gate outlive the pool and every worker using them.
    std::array<std::atomic<int>, 24> visits{};
    std::promise<void> releaseWork;
    auto gate = releaseWork.get_future().share();
    std::promise<void> submitted;
    auto submittedFuture = submitted.get_future();

    // Destroy this pool on another thread so the caller can release blocked work.
    auto shutdown = std::async(std::launch::async, [&] {
        ThreadPool pool{1};
        for (std::size_t job = 0; job < visits.size(); ++job)
        {
            pool.push({
                [&visits, gate, job](std::vector<Parameter> const&) {
                    gate.wait();
                    ++visits[job];
                },
                {}
            });
        }
        submitted.set_value();
        // No pool.wait(): leaving this scope must drain the queue and join workers.
    });

    const bool acceptedAll = submittedFuture.wait_for(std::chrono::seconds{5})
        == std::future_status::ready;
    const bool workStillPending = std::all_of(visits.begin(), visits.end(),
        [](const std::atomic<int>& count) { return count.load() == 0; });
    // A bounded observation checks that destruction does not finish while work
    // is blocked. Release the gate even if this check fails, so cleanup can proceed.
    const bool shutdownPending = shutdown.wait_for(std::chrono::milliseconds{50})
        == std::future_status::timeout;
    const bool waitedForWork = acceptedAll && workStillPending && shutdownPending;
    releaseWork.set_value();

    bool shutdownSucceeded = true;
    try
    {
        shutdown.get(); // Ready only after the local pool's destructor returns.
    }
    catch (...)
    {
        shutdownSucceeded = false;
    }

    const bool drained = shutdownSucceeded
        && std::all_of(visits.begin(), visits.end(),
            [](const std::atomic<int>& count) { return count.load() == 1; });
    std::cout << "[" << (waitedForWork ? "PASS" : "FAIL")
              << "] shutdown: destruction waits for pending work\n";
    std::cout << "[" << (drained ? "PASS" : "FAIL")
              << "] shutdown: all 24 tasks finish exactly once without wait()\n";

    // Empty queues must also wake and join their workers when closed.
    {
        ThreadPool idlePool{2};
    }
    std::cout << "[PASS] shutdown: an idle pool exits normally\n";
    return waitedForWork && drained;
}
#endif

#if JOBKIT_TEST_GRAPH_VALIDATION
static bool testGraphValidation()
{
    enum class Result { Accepted, Cycle, ExternalLink, UnexpectedError };
    auto check = [](const JobGraph& graph, std::string_view name, Result expected) {
        Result actual = Result::Accepted;
        try
        {
            graph.validate();
        }
        catch (const std::invalid_argument&)
        {
            actual = Result::ExternalLink;
        }
        catch (const std::runtime_error&)
        {
            actual = Result::Cycle;
        }
        catch (...)
        {
            actual = Result::UnexpectedError;
        }

        const bool passed = actual == expected;
        std::cout << "[" << (passed ? "PASS" : "FAIL")
                  << "] graph validation: " << name << '\n';
        return passed;
    };

    // Validation inspects links and counts; none of these operations are scheduled.
    const Task noOp{[](std::vector<Parameter> const&) {}, {}};
    bool passed = true;
    {
        JobGraph graph;
        passed = check(graph, "empty graph accepted", Result::Accepted) && passed;
    }
    {
        JobGraph graph;
        auto& a = graph.createJob(noOp);
        auto& b = graph.createJob(noOp);
        auto& c = graph.createJob(noOp);
        b.dependsOn(a);
        c.dependsOn(b);
        passed = check(graph, "chain accepted", Result::Accepted) && passed;
        // A second pass must see unchanged prerequisite counts.
        passed = check(graph, "repeat validation preserves counters", Result::Accepted) && passed;
    }
    {
        JobGraph graph;
        auto& a = graph.createJob(noOp);
        auto& b = graph.createJob(noOp);
        auto& c = graph.createJob(noOp);
        auto& d = graph.createJob(noOp);
        b.dependsOn(a);
        c.dependsOn(a);
        d.dependsOn(b);
        d.dependsOn(c);
        passed = check(graph, "diamond accepted", Result::Accepted) && passed;
    }
    {
        JobGraph graph;
        auto& a = graph.createJob(noOp);
        auto& b = graph.createJob(noOp);
        a.dependsOn(b);
        b.dependsOn(a);
        passed = check(graph, "two-node cycle rejected", Result::Cycle) && passed;
    }
    {
        // The same cross-graph link must be rejected from either end.
        // Keep both graphs alive while checking their stored node pointers.
        JobGraph source;
        JobGraph destination;
        auto& prerequisite = source.createJob(noOp);
        auto& dependent = destination.createJob(noOp);
        dependent.dependsOn(prerequisite);
        passed = check(source, "outgoing external link rejected", Result::ExternalLink) && passed;
        passed = check(destination, "incoming external prerequisite rejected", Result::ExternalLink) && passed;
    }
    {
        JobGraph graph;
        JobGraph external;
        auto& a = graph.createJob(noOp);
        auto& b = graph.createJob(noOp);
        auto& dependent = external.createJob(noOp);
        a.dependsOn(b);
        b.dependsOn(a);
        dependent.dependsOn(a);
        // No roots exist here: the link precheck must run before traversal.
        passed = check(graph, "external link in cyclic section rejected",
            Result::ExternalLink) && passed;
    }
    return passed;
}
#endif

int main()
{
    // Locals are destroyed in reverse order: the output mutex must outlive the pool.
    std::mutex outputMutex;
    ThreadPool pool{4};
    bool passed = true;

#if JOBKIT_TEST_PUSH
    passed = testPush(pool, outputMutex) && passed;
#endif

#if JOBKIT_TEST_DISPATCH
    passed = testDispatch(pool, outputMutex) && passed;
#endif

#if JOBKIT_TEST_EXCEPTIONS
    passed = testExceptions(pool) && passed;
#endif

#if JOBKIT_TEST_GROUPS
    passed = testGroups(pool, outputMutex) && passed;
#endif

#if JOBKIT_TEST_INDEPENDENT_GROUPS
    passed = testIndependentGroups(pool, outputMutex) && passed;
#endif

#if JOBKIT_TEST_SHUTDOWN
    passed = testShutdown() && passed;
#endif

#if JOBKIT_TEST_GRAPH_VALIDATION
    passed = testGraphValidation() && passed;
#endif

    std::cout << "All enabled tests finished: " << (passed ? "PASS" : "FAIL") << '\n';
    return passed ? 0 : 1;
}
