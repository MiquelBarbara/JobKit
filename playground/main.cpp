#include <JobKit/JobGraph.h>
#include <JobKit/JobGroup.h>
#include <JobKit/ThreadPool.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>
#endif

// Set a switch to 0 and rebuild to disable its tests.
#ifndef JOBKIT_TEST_GRAPH_DESTRUCTION
#define JOBKIT_TEST_GRAPH_DESTRUCTION 1
#endif

#ifndef JOBKIT_TEST_WORKER_DESTRUCTION_GUARD
#define JOBKIT_TEST_WORKER_DESTRUCTION_GUARD 1
#endif

#ifndef JOBKIT_TEST_WORKER_WAIT_GUARDS
#define JOBKIT_TEST_WORKER_WAIT_GUARDS 1
#endif

#ifndef JOBKIT_TEST_GRAPH_COMPLETIONS
#define JOBKIT_TEST_GRAPH_COMPLETIONS 1
#endif

#ifndef JOBKIT_TEST_GRAPH_VALIDATION_LIFECYCLE
#define JOBKIT_TEST_GRAPH_VALIDATION_LIFECYCLE 1
#endif

#ifndef JOBKIT_TEST_GROUP_DESTRUCTION
#define JOBKIT_TEST_GROUP_DESTRUCTION 1
#endif

#ifndef JOBKIT_TEST_SUBMISSION_FAILURE
#define JOBKIT_TEST_SUBMISSION_FAILURE 1
#endif

static_assert(std::is_nothrow_destructible_v<JobKit::JobGraph>);
static_assert(std::is_nothrow_destructible_v<JobKit::JobGroup>);

static constexpr int expectedTerminateExit = 86;
static constexpr std::string_view workerDestructionMode = "--jobkit-worker-destruction-child";
static constexpr std::string_view poolWaitMode = "--jobkit-pool-wait-child";
static constexpr std::string_view groupWaitMode = "--jobkit-group-wait-child";
static constexpr std::string_view graphWaitMode = "--jobkit-graph-wait-child";
static constexpr std::string_view deepCompletionMode = "--jobkit-deep-completion-child";
static constexpr std::string_view deepDestructionMode = "--jobkit-deep-destruction-child";
static constexpr std::string_view wideCompletionMode = "--jobkit-wide-completion-child";
static constexpr std::string_view successfulJoinMode = "--jobkit-successful-join-child";
static constexpr std::string_view failedJoinMode = "--jobkit-failed-join-child";
static constexpr std::string_view completionLifetimeMode = "--jobkit-completion-lifetime-child";
static constexpr std::string_view pendingGroupMode = "--jobkit-pending-group-child";
static constexpr std::string_view failedGroupMode = "--jobkit-failed-group-child";
static constexpr std::string_view emptyWorkerGroupMode = "--jobkit-empty-worker-group-child";
static constexpr std::string_view completedWorkerGroupMode = "--jobkit-completed-worker-group-child";
static constexpr std::string_view unfinishedWorkerGroupMode = "--jobkit-unfinished-worker-group-child";
static constexpr std::string_view groupedSubmissionFailureMode = "--jobkit-grouped-submission-failure-child";
static constexpr std::string_view ungroupedSubmissionFailureMode = "--jobkit-ungrouped-submission-failure-child";

static bool report(bool passed, std::string_view name)
{
    std::cout << "[" << (passed ? "PASS" : "FAIL") << "] " << name << '\n';
    return passed;
}

#if JOBKIT_TEST_GRAPH_VALIDATION_LIFECYCLE
static bool rejectsValidationAfterSubmission(const JobKit::JobGraph& graph)
{
    try { graph.validate(); }
    catch (const std::logic_error& error)
    {
        // Check the guard's message: invalid_argument also derives from
        // logic_error, so a dependency-validation error must not pass this test.
        return std::string_view(error.what()) == "Cannot validate a JobGraph after submission";
    }
    catch (...) {}
    return false;
}

static bool testGraphValidationLifecycle(JobKit::ThreadPool& pool)
{
    std::array<std::atomic<int>, 3> visits{};
    std::promise<void> started;
    auto startedFuture = started.get_future();
    std::promise<void> releaseRoot;
    auto rootGate = releaseRoot.get_future().share();
    JobKit::JobGraph graph;
    auto& root = graph.createJob({
        [&, rootGate](std::vector<JobKit::Parameter> const&) {
            started.set_value();
            rootGate.wait();
            ++visits[0];
        }, {}});
    auto& child = graph.createJob({
        [&](std::vector<JobKit::Parameter> const&) { ++visits[1]; }, {}});
    child.dependsOn(root);

    bool validationAccepted = true;
    try
    {
        graph.validate();
        graph.validate(); // Validation must leave prerequisite counts unchanged.
    }
    catch (...) { validationAccepted = false; }
    // Validation must also leave the graph open for further construction.
    graph.createJob({
        [&](std::vector<JobKit::Parameter> const&) { ++visits[2]; }, {}});
    const bool before = report(validationAccepted
        && std::all_of(visits.begin(), visits.end(),
            [](const std::atomic<int>& count) { return count.load() == 0; }),
        "validation before submission is repeatable and still allows graph construction");

    bool runningRejected = false;
    try
    {
        graph.submit(pool);
        const bool rootStarted = startedFuture.wait_for(std::chrono::seconds{5})
            == std::future_status::ready;
        runningRejected = rejectsValidationAfterSubmission(graph)
            && rootStarted && visits[0].load() == 0 && visits[1].load() == 0;
    }
    catch (...)
    {
        // Release any accepted work before graph destruction during unwinding.
        releaseRoot.set_value();
        throw;
    }
    releaseRoot.set_value();
    bool executionSucceeded = true;
    try { graph.wait(); }
    catch (...) { executionSucceeded = false; }
    pool.wait();

    const bool during = report(runningRejected,
        "validation during blocked execution throws the lifecycle logic_error");
    const bool after = report(rejectsValidationAfterSubmission(graph)
        && executionSucceeded
        && std::all_of(visits.begin(), visits.end(),
            [](const std::atomic<int>& count) { return count.load() == 1; }),
        "validation after completion is rejected and every job still ran exactly once");
    return before && during && after;
}
#endif

#if JOBKIT_TEST_GRAPH_DESTRUCTION
static bool testPendingGraphDestruction(JobKit::ThreadPool& pool)
{
    // Captured data and the release signal outlive the graph and its worker tasks.
    std::array<std::atomic<int>, 3> visits{};
    std::promise<void> releaseRoot;
    auto rootGate = releaseRoot.get_future().share();
    std::promise<void> submitted;
    auto submittedFuture = submitted.get_future();

    // Destruction runs outside the pool so this thread can release the root.
    auto lifetime = std::async(std::launch::async, [&] {
        JobKit::JobGraph graph;
        auto& root = graph.createJob({
            [&visits, rootGate](std::vector<JobKit::Parameter> const&) {
                rootGate.wait();
                ++visits[0];
            }, {}});
        auto& child = graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) { ++visits[1]; }, {}});
        auto& grandchild = graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) { ++visits[2]; }, {}});
        child.dependsOn(root);
        grandchild.dependsOn(child);
        graph.submit(pool);
        submitted.set_value();
        // No graph.wait(): the destructor must keep all three nodes alive.
    });

    const bool submissionFinished = submittedFuture.wait_for(std::chrono::seconds{5})
        == std::future_status::ready;
    const bool noWorkFinished = std::all_of(visits.begin(), visits.end(),
        [](const std::atomic<int>& count) { return count.load() == 0; });
    const bool destructionPending = lifetime.wait_for(std::chrono::milliseconds{50})
        == std::future_status::timeout;

    // Always release before joining, including when an observation failed.
    releaseRoot.set_value();
    bool destructionSucceeded = true;
    try { lifetime.get(); }
    catch (...) { destructionSucceeded = false; }

    const bool waited = report(submissionFinished && noWorkFinished && destructionPending,
        "graph destructor waits while the root is blocked");
    const bool drained = report(destructionSucceeded
        && std::all_of(visits.begin(), visits.end(),
            [](const std::atomic<int>& count) { return count.load() == 1; }),
        "graph destructor finishes the root and both dependents without explicit wait");
    pool.wait(); // Drain wrapper bookkeeping before captured test data goes away.
    return waited && drained;
}

static bool testFailedGraphDestruction(JobKit::ThreadPool& pool)
{
    std::array<std::atomic<int>, 3> visits{};
    bool scopeExited = false;
    try
    {
        JobKit::JobGraph graph;
        auto& failed = graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) {
                ++visits[0];
                throw std::logic_error("Expected failure during graph destruction");
            }, {}});
        auto& skipped = graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) { ++visits[1]; }, {}});
        graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) { ++visits[2]; }, {}});
        skipped.dependsOn(failed);
        graph.submit(pool);
        // Scope exit must wait, skip the dependent, and avoid rethrowing task errors.
    }
    catch (...)
    {
        pool.wait();
        return report(false, "graph destruction does not report task failures");
    }
    scopeExited = true;

    const bool passed = report(scopeExited && visits[0].load() == 1
        && visits[1].load() == 0 && visits[2].load() == 1,
        "graph destruction handles failure, skipped work, and unrelated work");
    pool.wait();
    return passed;
}

static bool testUnsubmittedGraphDestruction()
{
    std::atomic<int> visits{0};
    {
        JobKit::JobGraph graph;
        graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) { ++visits; }, {}});
    }
    return report(visits.load() == 0, "unsubmitted graph destruction executes no work");
}

static bool testExplicitWaitStillReportsErrors(JobKit::ThreadPool& pool)
{
    JobKit::JobGraph graph;
    graph.createJob({
        [](std::vector<JobKit::Parameter> const&) {
            throw std::logic_error("Expected explicit wait failure");
        }, {}});
    graph.submit(pool);

    bool originalError = false;
    try { graph.wait(); }
    catch (const std::logic_error& error) {
        originalError = std::string_view(error.what()) == "Expected explicit wait failure";
    }
    catch (...) {}

    pool.wait();
    return report(originalError, "explicit graph wait preserves the original task exception");
}

static bool testWorkerThreadDetection(JobKit::ThreadPool& pool)
{
    const bool callerIsNotWorker = !JobKit::ThreadPool::isWorkerThread();
    std::atomic<bool> workerDetected{false};
    pool.push({
        [&](std::vector<JobKit::Parameter> const&) {
            workerDetected.store(JobKit::ThreadPool::isWorkerThread());
        }, {}});
    pool.wait();
    return report(callerIsNotWorker && workerDetected.load(),
        "worker marker distinguishes the caller from pool workers");
}
#endif

// This mode is run ONLY in a child process. Even a completed submitted graph is
// forbidden on a worker under the current destruction policy.
static int runWorkerDestructionChild()
{
    JobKit::ThreadPool pool{1};
    auto graph = std::make_unique<JobKit::JobGraph>();
    graph->createJob({[](std::vector<JobKit::Parameter> const&) {}, {}});
    graph->submit(pool);
    graph->wait();
    JobKit::JobGraph* ownedGraph = graph.release();
    pool.push({
        [ownedGraph](std::vector<JobKit::Parameter> const&) {
            if (!JobKit::ThreadPool::isWorkerThread()) std::_Exit(89);
            // Convert the intentional termination into an identifiable exit code.
            std::set_terminate([] { std::_Exit(expectedTerminateExit); });
            delete ownedGraph;
            std::_Exit(87); // The destructor incorrectly returned instead of terminating.
        }, {}});
    pool.wait();
    return 88;
}

// A missing guard would deadlock on the task's own unfinished count. Running
// each case in a child lets the parent report failure after a bounded wait.
static int runWorkerWaitChild(std::string_view mode)
{
    JobKit::ThreadPool pool{1};
    JobKit::JobGroup group;
    JobKit::JobGraph graph;
    std::atomic<bool> rejected{false};
    std::atomic<bool> continued{false};
    auto attemptWait = [&](std::vector<JobKit::Parameter> const&) {
        try
        {
            if (mode == poolWaitMode) pool.wait();
            else if (mode == groupWaitMode) group.wait();
            else graph.wait();
        }
        catch (const std::logic_error&) { rejected.store(true); }
        catch (...) {} // A different exception does not satisfy the contract.
        // Catch inside the task so the caller's wait must still succeed.
    };

    if (mode == graphWaitMode)
    {
        graph.createJob({attemptWait, {}});
        graph.submit(pool);
        graph.wait();
    }
    else
    {
        pool.push({attemptWait, {}, mode == groupWaitMode ? &group : nullptr});
        if (mode == groupWaitMode) group.wait();
    }
    pool.wait();

    // Rejecting the wait must leave the worker available for subsequent work.
    pool.push({
        [&](std::vector<JobKit::Parameter> const&) { continued.store(true); }, {}});
    pool.wait();
    return rejected.load() && continued.load() ? 0 : 1;
}

#if JOBKIT_TEST_GRAPH_COMPLETIONS
static constexpr std::string_view completionError = "Expected completion failure";

template<typename Waitable>
static bool checkCompletionWait(Waitable& target, bool expectFailure)
{
    try { target.wait(); }
    catch (const std::runtime_error& error)
    {
        return expectFailure && std::string_view(error.what()) == completionError;
    }
    catch (...) { return false; }
    return !expectFailure;
}

static int runFailurePropagationChild(bool wide, bool destroyWithoutWait)
{
    JobKit::ThreadPool pool{4};
    JobKit::JobGroup group;
    std::atomic<int> rootVisits{0};
    std::atomic<int> skippedVisits{0};
    std::atomic<int> independentVisits{0};
    bool passed = true;
    {
        JobKit::JobGraph graph;
        auto& root = graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) {
                ++rootVisits;
                throw std::runtime_error(std::string(completionError));
            }, {}, &group});
        JobKit::JobNode* previous = &root;
        // A deep chain exposes recursive stack growth. A wide graph fills the
        // pending worklist while processing the failed root's dependents.
        const std::size_t descendantCount = wide ? 20000 : 100000;
        for (std::size_t i = 0; i < descendantCount; ++i)
        {
            auto& child = graph.createJob({
                [&](std::vector<JobKit::Parameter> const&) { ++skippedVisits; }, {}, &group});
            child.dependsOn(wide ? root : *previous);
            previous = &child;
        }
        graph.createJob({
            [&](std::vector<JobKit::Parameter> const&) { ++independentVisits; }, {}, &group});
        graph.submit(pool);
        if (!destroyWithoutWait)
        {
            passed = checkCompletionWait(graph, true);
            // The reported error is consumed; the graph remains completed.
            passed = checkCompletionWait(graph, false) && passed;
        }
        // In the destruction case, scope exit must finish the entire cascade.
    }
    passed = checkCompletionWait(group, true) && passed;
    passed = checkCompletionWait(group, false) && passed;
    pool.wait();
    return passed && rootVisits.load() == 1 && skippedVisits.load() == 0
        && independentVisits.load() == 1 ? 0 : 1;
}

static int runConcurrentJoinChild(bool failRoot)
{
    bool passed = true;
    for (const std::size_t workerCount : {1u, 2u, 4u, 8u})
    {
        JobKit::ThreadPool pool{workerCount};
        for (int round = 0; round < 16; ++round)
        {
            JobKit::JobGroup group;
            std::array<std::atomic<int>, 8> rootVisits{};
            std::atomic<int> joinVisits{0};
            std::atomic<int> tailVisits{0};
            std::atomic<int> independentVisits{0};
            std::atomic<int> independentTailVisits{0};
            std::atomic<bool> prerequisitesFinished{true};
            // Exactly workerCount roots participate, so every participant can
            // run. This aligns their completion without relying on sleeps.
            std::barrier rootGate{static_cast<std::ptrdiff_t>(workerCount)};
            JobKit::JobGraph graph;
            std::array<JobKit::JobNode*, 8> roots{};
            for (std::size_t i = 0; i < workerCount; ++i)
            {
                roots[i] = &graph.createJob({
                    [&, i](std::vector<JobKit::Parameter> const&) {
                        rootGate.arrive_and_wait();
                        ++rootVisits[i];
                        if (failRoot && i == 0)
                            throw std::runtime_error(std::string(completionError));
                    }, {}, &group});
            }
            auto& join = graph.createJob({
                [&](std::vector<JobKit::Parameter> const&) {
                    for (std::size_t i = 0; i < workerCount; ++i)
                        if (rootVisits[i].load() != 1) prerequisitesFinished.store(false);
                    ++joinVisits;
                }, {}, &group});
            for (std::size_t i = 0; i < workerCount; ++i) join.dependsOn(*roots[i]);
            auto& tail = graph.createJob({
                [&](std::vector<JobKit::Parameter> const&) { ++tailVisits; }, {}, &group});
            tail.dependsOn(join);
            auto& independent = graph.createJob({
                [&](std::vector<JobKit::Parameter> const&) { ++independentVisits; }, {}, &group});
            auto& independentTail = graph.createJob({
                [&](std::vector<JobKit::Parameter> const&) { ++independentTailVisits; }, {}, &group});
            independentTail.dependsOn(independent);

            graph.submit(pool);
            passed = checkCompletionWait(graph, failRoot) && passed;
            passed = checkCompletionWait(group, failRoot) && passed;
            pool.wait();
            for (std::size_t i = 0; i < workerCount; ++i)
                passed = rootVisits[i].load() == 1 && passed;
            const int expectedJoinVisits = failRoot ? 0 : 1;
            passed = prerequisitesFinished.load() && joinVisits.load() == expectedJoinVisits
                && tailVisits.load() == expectedJoinVisits
                && independentVisits.load() == 1 && independentTailVisits.load() == 1 && passed;
        }
    }
    return passed ? 0 : 1;
}

static int runCompletionLifetimeChild()
{
    JobKit::ThreadPool pool{4};
    for (int round = 0; round < 1000; ++round)
    {
        JobKit::JobGroup group;
        std::atomic<int> visits{0};
        auto graph = std::make_unique<JobKit::JobGraph>();
        graph->createJob({
            [&](std::vector<JobKit::Parameter> const&) { ++visits; }, {}, &group});
        graph->submit(pool);
        group.wait();
        // The group can finish before the drainer exits. Destroy immediately:
        // the graph's own wait must include the drainer's extra count.
        graph.reset();
        pool.wait();
        if (visits.load() != 1) return 1;
    }
    return 0;
}
#endif

#if JOBKIT_TEST_GROUP_DESTRUCTION
static int runPendingGroupDestructionChild()
{
    // Unexpected termination becomes a failing child exit without an OS dialog.
    std::set_terminate([] { std::_Exit(expectedTerminateExit); });
    JobKit::ThreadPool pool{1};
    std::array<std::atomic<int>, 2> visits{};
    std::promise<void> releaseRoot;
    auto rootGate = releaseRoot.get_future().share();
    std::promise<void> started;
    auto startedFuture = started.get_future();
    std::promise<void> submitted;
    auto submittedFuture = submitted.get_future();
    auto lifetime = std::async(std::launch::async, [&] {
        JobKit::JobGroup group;
        pool.push({
            [&, rootGate](std::vector<JobKit::Parameter> const&) {
                started.set_value();
                rootGate.wait();
                ++visits[0];
            }, {}, &group});
        pool.push({
            [&](std::vector<JobKit::Parameter> const&) { ++visits[1]; }, {}, &group});
        submitted.set_value();
        // No group.wait(): scope exit must cover both running and queued work.
    });

    const bool submittedBoth = submittedFuture.wait_for(std::chrono::seconds{5})
        == std::future_status::ready;
    const bool rootStarted = startedFuture.wait_for(std::chrono::seconds{5})
        == std::future_status::ready;
    const bool stillWaiting = lifetime.wait_for(std::chrono::milliseconds{50})
        == std::future_status::timeout;
    const bool neitherFinished = visits[0].load() == 0 && visits[1].load() == 0;
    releaseRoot.set_value(); // Always unblock before joining, even on failure.
    bool destructionSucceeded = true;
    try { lifetime.get(); }
    catch (...) { destructionSucceeded = false; }
    const bool completedOnDestruction = visits[0].load() == 1 && visits[1].load() == 1;
    pool.wait(); // Group completion precedes the pool's final bookkeeping.
    return submittedBoth && rootStarted && stillWaiting && neitherFinished
        && destructionSucceeded && completedOnDestruction ? 0 : 1;
}

static int runFailedGroupDestructionChild()
{
    std::set_terminate([] { std::_Exit(expectedTerminateExit); });
    JobKit::ThreadPool pool{2};
    std::atomic<int> visits{0};
    {
        JobKit::JobGroup group;
        pool.push({
            [&](std::vector<JobKit::Parameter> const&) {
                ++visits;
                throw std::runtime_error("Expected group destruction failure");
            }, {}, &group});
        pool.push({
            [&](std::vector<JobKit::Parameter> const&) { ++visits; }, {}, &group});
        // Destruction must wait for both tasks without reporting their errors.
    }
    const bool completedOnDestruction = visits.load() == 2;
    bool originalPoolError = false;
    try { pool.wait(); }
    catch (const std::runtime_error& error)
    {
        originalPoolError = std::string_view(error.what()) == "Expected group destruction failure";
    }
    catch (...) {}
    std::atomic<bool> continued{false};
    pool.push({
        [&](std::vector<JobKit::Parameter> const&) { continued.store(true); }, {}});
    pool.wait();
    return completedOnDestruction && originalPoolError && continued.load() ? 0 : 1;
}

static int runCompletedWorkerGroupChild(bool previouslyUsed)
{
    std::set_terminate([] { std::_Exit(expectedTerminateExit); });
    JobKit::ThreadPool pool{1};
    auto group = std::make_unique<JobKit::JobGroup>();
    std::atomic<int> visits{0};
    if (previouslyUsed)
    {
        pool.push({
            [&](std::vector<JobKit::Parameter> const&) { ++visits; }, {}, group.get()});
        group->wait();
        pool.wait();
    }
    JobKit::JobGroup* ownedGroup = group.release();
    std::atomic<bool> destroyedOnWorker{false};
    pool.push({
        [&, ownedGroup](std::vector<JobKit::Parameter> const&) {
            if (!JobKit::ThreadPool::isWorkerThread()) std::_Exit(89);
            delete ownedGroup;
            destroyedOnWorker.store(true);
        }, {}}); // The deleting task does not belong to the group it destroys.
    pool.wait();
    return destroyedOnWorker.load() && visits.load() == (previouslyUsed ? 1 : 0) ? 0 : 1;
}

static int runUnfinishedWorkerGroupChild()
{
    JobKit::ThreadPool pool{1};
    auto* group = new JobKit::JobGroup;
    pool.push({
        [group](std::vector<JobKit::Parameter> const&) {
            if (!JobKit::ThreadPool::isWorkerThread()) std::_Exit(89);
            // This task itself keeps the group's unfinished count above zero.
            std::set_terminate([] { std::_Exit(expectedTerminateExit); });
            delete group;
            std::_Exit(87); // Fail without letting the pool access a deleted group.
        }, {}, group});
    pool.wait();
    return 88;
}
#endif

#if JOBKIT_TEST_SUBMISSION_FAILURE
struct SubmissionCopyState
{
    bool throwOnCopy = false;
    int failedCopies = 0;
    std::atomic<int> executions{0};
};

struct ExpectedSubmissionFailure final : std::exception
{
    explicit ExpectedSubmissionFailure(const SubmissionCopyState* source) : source(source) {}
    const char* what() const noexcept override { return "Expected task copy failure"; }
    const SubmissionCopyState* source;
};

class ThrowingCopyCallable
{
public:
    explicit ThrowingCopyCallable(SubmissionCopyState& state) : _state(&state) {}
    ThrowingCopyCallable(ThrowingCopyCallable&&) noexcept = default;
    ThrowingCopyCallable(const ThrowingCopyCallable& other) : _state(other._state)
    {
        if (_state->throwOnCopy)
        {
            ++_state->failedCopies;
            throw ExpectedSubmissionFailure{_state};
        }
    }
    void operator()(std::vector<JobKit::Parameter> const&) const
    {
        ++_state->executions; // Execution itself never throws.
    }

private:
    SubmissionCopyState* _state;
};

static int runSubmissionFailureChild(bool grouped)
{
    std::set_terminate([] { std::_Exit(expectedTerminateExit); });
    SubmissionCopyState state;
    JobKit::ThreadPool pool{1};
    JobKit::JobGroup group;
    JobKit::Task task{ThrowingCopyCallable{state}, {}, grouped ? &group : nullptr};

    // Arm only after constructing Task: the failure must happen inside push(),
    // when std::function copies its target into the queue's Task entry.
    state.throwOnCopy = true;
    bool originalException = false;
    try { pool.push(task); }
    catch (const ExpectedSubmissionFailure& error)
    {
        originalException = error.source == &state;
    }
    catch (...) {}
    state.throwOnCopy = false;

    auto waitSucceeds = [](auto& target) {
        try { target.wait(); return true; }
        catch (...) { return false; }
    };
    // A leaked group or pool count deadlocks here; the parent bounds the child.
    bool passed = originalException && state.failedCopies == 1;
    passed = waitSucceeds(group) && passed;
    passed = waitSucceeds(pool) && passed;
    passed = state.executions.load() == 0 && passed;

    // Reuse the same Task, pool, and group after disabling the injected failure.
    const bool accepted = pool.push(task);
    passed = waitSucceeds(group) && passed;
    passed = waitSucceeds(pool) && passed;
    return passed && accepted && state.executions.load() == 1 ? 0 : 1;
}
#endif

#if JOBKIT_TEST_WORKER_DESTRUCTION_GUARD || JOBKIT_TEST_WORKER_WAIT_GUARDS || JOBKIT_TEST_GRAPH_COMPLETIONS || JOBKIT_TEST_GROUP_DESTRUCTION || JOBKIT_TEST_SUBMISSION_FAILURE
static bool testIsolatedChild(std::string_view mode, unsigned long expectedExit,
    std::string_view name)
{
#ifdef _WIN32
    std::wstring executable(32768, L'\0');
    const DWORD length = GetModuleFileNameW(nullptr, executable.data(),
        static_cast<DWORD>(executable.size()));
    if (length == 0 || length >= executable.size())
        return report(false, name);
    executable.resize(length);
    std::wstring command = L"\"" + executable + L"\" ";
    command.append(mode.begin(), mode.end());
    STARTUPINFOW startup{};
    startup.cb = static_cast<DWORD>(sizeof(startup));
    PROCESS_INFORMATION process{};
    if (!CreateProcessW(executable.c_str(), command.data(), nullptr, nullptr, FALSE,
        CREATE_NO_WINDOW, nullptr, nullptr, &startup, &process))
        return report(false, name);
    CloseHandle(process.hThread);

    const DWORD waitResult = WaitForSingleObject(process.hProcess, 10000);
    if (waitResult != WAIT_OBJECT_0)
    {
        // Only the child created above is terminated if the guard deadlocks.
        TerminateProcess(process.hProcess, 90);
        WaitForSingleObject(process.hProcess, 5000);
    }
    DWORD exitCode = 0;
    const bool gotExitCode = GetExitCodeProcess(process.hProcess, &exitCode) != FALSE;
    CloseHandle(process.hProcess);
    return report(waitResult == WAIT_OBJECT_0 && gotExitCode
        && exitCode == expectedExit, name);
#else
    std::cout << "[SKIP] " << name << ": isolated process check requires Windows\n";
    return true;
#endif
}
#endif

int main(int argc, char* argv[])
{
    try
    {
        if (argc == 2 && std::string_view(argv[1]) == workerDestructionMode)
            return runWorkerDestructionChild();
        if (argc == 2 && (std::string_view(argv[1]) == poolWaitMode
            || std::string_view(argv[1]) == groupWaitMode
            || std::string_view(argv[1]) == graphWaitMode))
            return runWorkerWaitChild(argv[1]);
#if JOBKIT_TEST_SUBMISSION_FAILURE
        if (argc == 2)
        {
            const std::string_view mode = argv[1];
            if (mode == groupedSubmissionFailureMode) return runSubmissionFailureChild(true);
            if (mode == ungroupedSubmissionFailureMode) return runSubmissionFailureChild(false);
        }
#endif
#if JOBKIT_TEST_GROUP_DESTRUCTION
        if (argc == 2)
        {
            const std::string_view mode = argv[1];
            if (mode == pendingGroupMode) return runPendingGroupDestructionChild();
            if (mode == failedGroupMode) return runFailedGroupDestructionChild();
            if (mode == emptyWorkerGroupMode) return runCompletedWorkerGroupChild(false);
            if (mode == completedWorkerGroupMode) return runCompletedWorkerGroupChild(true);
            if (mode == unfinishedWorkerGroupMode) return runUnfinishedWorkerGroupChild();
        }
#endif
#if JOBKIT_TEST_GRAPH_COMPLETIONS
        if (argc == 2)
        {
#ifdef _WIN32
            // A stack-overflow regression should fail the child without an OS dialog.
            SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
#endif
            const std::string_view mode = argv[1];
            if (mode == deepCompletionMode) return runFailurePropagationChild(false, false);
            if (mode == deepDestructionMode) return runFailurePropagationChild(false, true);
            if (mode == wideCompletionMode) return runFailurePropagationChild(true, false);
            if (mode == successfulJoinMode) return runConcurrentJoinChild(false);
            if (mode == failedJoinMode) return runConcurrentJoinChild(true);
            if (mode == completionLifetimeMode) return runCompletionLifetimeChild();
        }
#endif

        JobKit::ThreadPool pool{4};
        bool passed = true;
#if JOBKIT_TEST_GRAPH_DESTRUCTION
        passed = testWorkerThreadDetection(pool) && passed;
        passed = testPendingGraphDestruction(pool) && passed;
        passed = testFailedGraphDestruction(pool) && passed;
        passed = testUnsubmittedGraphDestruction() && passed;
        passed = testExplicitWaitStillReportsErrors(pool) && passed;
#endif
#if JOBKIT_TEST_GRAPH_VALIDATION_LIFECYCLE
        passed = testGraphValidationLifecycle(pool) && passed;
#endif
#if JOBKIT_TEST_WORKER_DESTRUCTION_GUARD
        passed = testIsolatedChild(workerDestructionMode, expectedTerminateExit,
            "worker graph destruction terminates in the isolated child") && passed;
#endif
#if JOBKIT_TEST_WORKER_WAIT_GUARDS
        passed = testIsolatedChild(poolWaitMode, 0,
            "worker pool.wait() throws logic_error; caller waits and worker continues") && passed;
        passed = testIsolatedChild(groupWaitMode, 0,
            "worker group.wait() throws logic_error; caller waits and worker continues") && passed;
        passed = testIsolatedChild(graphWaitMode, 0,
            "worker graph.wait() throws logic_error; caller waits and worker continues") && passed;
#endif
#if JOBKIT_TEST_GRAPH_COMPLETIONS
        passed = testIsolatedChild(deepCompletionMode, 0,
            "failed root skips 100000 chained descendants and preserves graph/group errors") && passed;
        passed = testIsolatedChild(deepDestructionMode, 0,
            "graph destruction drains 100000 skipped descendants without explicit wait") && passed;
        passed = testIsolatedChild(wideCompletionMode, 0,
            "failed root skips 20000 direct dependents and unrelated work continues") && passed;
        passed = testIsolatedChild(successfulJoinMode, 0,
            "concurrent successful prerequisites run the join and tail exactly once (1/2/4/8 workers)") && passed;
        passed = testIsolatedChild(failedJoinMode, 0,
            "concurrent mixed prerequisites skip the join and tail; unrelated branch finishes") && passed;
        passed = testIsolatedChild(completionLifetimeMode, 0,
            "immediate graph destruction after grouped completion (1000 iterations)") && passed;
#endif
#if JOBKIT_TEST_GROUP_DESTRUCTION
        passed = testIsolatedChild(pendingGroupMode, 0,
            "group destruction waits for running and queued tasks without explicit wait") && passed;
        passed = testIsolatedChild(failedGroupMode, 0,
            "group destruction drains failed tasks without rethrowing; pool preserves the error") && passed;
        passed = testIsolatedChild(emptyWorkerGroupMode, 0,
            "worker can destroy an empty group") && passed;
        passed = testIsolatedChild(completedWorkerGroupMode, 0,
            "worker can destroy a previously used completed group") && passed;
        passed = testIsolatedChild(unfinishedWorkerGroupMode, expectedTerminateExit,
            "worker destruction of an unfinished group terminates in the isolated child") && passed;
#endif
#if JOBKIT_TEST_SUBMISSION_FAILURE
        passed = testIsolatedChild(groupedSubmissionFailureMode, 0,
            "grouped submission copy failure preserves the exception, rolls back counts, and allows reuse") && passed;
        passed = testIsolatedChild(ungroupedSubmissionFailureMode, 0,
            "ungrouped submission copy failure executes no work and allows pool reuse") && passed;
#endif
        std::cout << "All enabled tests finished: " << (passed ? "PASS" : "FAIL") << '\n';
        return passed ? 0 : 1;
    }
    catch (const std::exception& error)
    {
        std::cerr << "[FAIL] Test setup failed: " << error.what() << '\n';
        return 1;
    }
}
