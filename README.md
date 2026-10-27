# JobKit

JobKit is a C++20 job system being developed for use in a game engine. It provides a fixed thread pool, batched work, completion groups, and dependency graphs.

The current implementation is ready for an initial engine integration with the ownership and shutdown rules below. The latest x64 Release verification passed all 26 playground checks. Debug coverage, memory/race analysis, and engine workload benchmarks remain to be done.

## Build and run

The supplied Visual Studio projects use the v145 toolset, a Windows SDK, and C++20. They define Debug and Release configurations for Win32 and x64.

1. Open `JobKit.slnx` in a Visual Studio installation supporting these projects.
2. Select a configuration and platform, such as Release / x64.
3. Build the solution and run `JobKit.Playground` to execute the enabled checks.

From a Visual Studio Developer PowerShell, the equivalent build and run commands are:

```powershell
msbuild .\JobKit.slnx /m:1 /p:Configuration=Release /p:Platform=x64
.\out\x64\Release\JobKit.Playground.exe
```

`JobKit.vcxproj` builds a static library. To consume it, add a project reference, add `include` to the consumer's include paths, and use headers such as `<JobKit/ThreadPool.h>`. Match the consumer's platform, configuration, and C++ runtime settings. The playground project is an example consumer.

```text
include/JobKit/   Public headers
source/          Library implementations
playground/      Console test application
out/             Build outputs and intermediate files
```

## Public API

All library types belong to the `JobKit` namespace.

| Type | Purpose |
| --- | --- |
| `ThreadPool` | Owns a fixed number of workers; provides `push()`, `dispatch()`, and `wait()`. Construction requires at least one worker. |
| `Task` | Holds a callable, its argument vector, and an optional non-owning `JobGroup*`. |
| `JobGroup` | Tracks completion of a selected set of tasks independently of other pool work. |
| `JobGraph` | Owns jobs, validates dependencies, and submits a graph once. |
| `JobNode` | Represents a graph job. `child.dependsOn(parent)` makes the child wait for the parent. |
| `ThreadQueue<T>` | Provides synchronized FIFO storage; closing it rejects pushes and lets consumers drain existing entries. |

`Task::task` has the signature `void(std::vector<Parameter>)`. `Parameter` is a variant of `int`, `float`, `std::string`, and `bool`. Callables can also capture application data. The current task representation copies the argument vector when invoking a callable.

`push()` returns `true` when submission succeeds and throws if submission fails. A rejected task does not remain counted as unfinished work.

## Batched work

`dispatch(elementCount, batchSize, operation, group)` submits one task per batch. Each operation receives a half-open range `[begin, end)` and processes the elements in that range itself. The final batch may be smaller. An element count or batch size of zero submits no work.

```cpp
#include <JobKit/JobGroup.h>
#include <JobKit/ThreadPool.h>
#include <cstddef>
#include <vector>

int main()
{
    // Data outlives both the group and the pool.
    std::vector<int> output(1000);
    JobKit::ThreadPool pool{4};
    JobKit::JobGroup group;

    pool.dispatch(output.size(), 128,
        [&output](std::size_t begin, std::size_t end) {
            for (std::size_t i = begin; i < end; ++i)
                output[i] = static_cast<int>(i * 2);
        }, &group);

    group.wait(); // All output elements are now ready to read.
    pool.wait();
}
```

Batch submission is incremental. If a later batch submission throws, previously accepted batches still run. Keep their data and group alive and finish waiting for accepted work before cleanup.

## Dependency graphs

Build a graph on one controlling thread, then submit it once. Graph construction, validation, and submission must not run concurrently on the same graph.

```cpp
#include <JobKit/JobGraph.h>
#include <JobKit/ThreadPool.h>
#include <array>
#include <cstddef>
#include <vector>

int main()
{
    std::array<int, 4> input{};
    std::array<int, 4> output{};
    JobKit::ThreadPool pool{4};
    JobKit::JobGraph graph;

    auto& prepare = graph.createJob({
        [&input](const std::vector<JobKit::Parameter>&) {
            input = {1, 2, 3, 4};
        }, {}});

    auto& transform = graph.createJob({
        [&input, &output](const std::vector<JobKit::Parameter>&) {
            for (std::size_t i = 0; i < input.size(); ++i)
                output[i] = input[i] * 2;
        }, {}});

    transform.dependsOn(prepare);
    graph.validate(); // Optional: submit() also validates before execution.
    graph.submit(pool);
    graph.wait();
    pool.wait();
}
```

The graph lifecycle is **build and validate -> submit once -> wait -> destroy**:

- Validation can be repeated before submission. It rejects cycles and dependencies outside the graph. Self-dependencies are rejected by `dependsOn()`.
- After submission, adding jobs or dependencies, submitting again, and calling `validate()` are rejected. Calling `validate()` after submission throws `std::logic_error`, including after completion.
- A node runs only after all prerequisites finish successfully. A failed prerequisite causes its dependent to be skipped; skipping propagates through descendants. Unrelated branches continue.
- Skipped and failed jobs still count toward completion. A graph-owned, preallocated worklist processes completions iteratively, avoiding recursive stack growth through long skipped chains.
- Node references are valid only while their owning graph exists. Create graph jobs through `createJob()` and keep every dependency inside that graph.

## Waiting and exceptions

| Operation | Completion and error behavior |
| --- | --- |
| `pool.wait()` | Waits for all unfinished pool entries, including queued and executing tasks, then reports the first recorded pool-task exception. |
| `group.wait()` | Waits for the group's registered tasks, then reports its first recorded member-task exception. |
| `graph.wait()` | Waits for every graph node and active completion processing, then reports the first recorded graph failure. |

Each wait consumes the exception stored in that object. With multiple failures, the recorded failure depends on execution order. Direct pool tasks can record the same failure independently in the pool and their group.

Graph jobs report their errors through the graph and any assigned groups. `pool.wait()` does not replace `graph.wait()` for observing graph failures. A group containing only skipped descendants may have no member-task exception to report; use `graph.wait()` to observe the prerequisite failure.

All three public waits throw `std::logic_error` when called from any JobKit worker, even if the target is already idle or belongs to another pool. Blocking on completion belongs to the controlling thread. Stop external submissions before treating a completed wait as a stable shutdown boundary; waiting does not prevent future submissions.

Submission can throw before a task is accepted. `push()` rolls back group registration if queue insertion fails and increments the pool count only after successful insertion. Graph scheduling failures mark affected nodes failed and propagate skipping; observe them through `graph.wait()`.

## Ownership and destruction

The library synchronizes task scheduling and completion. Application code remains responsible for safe access to data used by its tasks.

- A task's group pointer is non-owning. Keep the group alive from task submission until completion. For graph jobs, keep assigned groups alive from graph construction through execution.
- Capturing an object by reference or pointer does not extend its lifetime. Keep captured data alive until every task using it has finished, including during exception cleanup.
- Keep the pool alive until all submitted graphs finish. Graph completion may need to submit newly ready dependent jobs to that pool.
- Stop producers before destroying objects they use. Destructors protect already registered work; they do not make concurrent destruction and new submissions safe.

| Object | Destruction behavior |
| --- | --- |
| `JobGroup` | Outside workers, waits for registered work without reporting task exceptions. On a worker, an unfinished group causes `std::terminate()`; an empty or completed group is allowed. |
| `JobGraph` | Outside workers, waits for a submitted graph without reporting task exceptions. Destroying any submitted graph on a worker causes `std::terminate()`, even after completion. An unsubmitted graph executes no work during destruction. |
| `ThreadPool` | Closes the queue and joins its workers while they drain accepted entries. Destroy it from outside JobKit workers, after submitted graphs finish. |

Call the public waits explicitly when task failures must be handled; group and graph destructors do not report those failures.

## Engine shutdown contract

Perform shutdown from a controlling thread in this order:

1. Stop external job producers. Already submitted graphs must still be able to schedule their dependents.
2. Wait for every active graph and group, handling each reported error while continuing cleanup of the remaining work.
3. Call `pool.wait()` and handle any independently recorded pool-task error.
4. Destroy completed graphs and groups while the pool still exists.
5. Destroy the pool, joining its workers.
6. Release engine resources accessed by jobs.

The pool destructor closes its queue immediately. Destroying the pool with an unfinished graph can reject dependent-job submissions and turn otherwise runnable graph jobs into failures. Queue draining alone is not a guarantee that all graph callbacks will execute.

The pool currently exposes no separate public stop-accepting or shutdown operation; the engine must control producers and object lifetimes explicitly.

## Tests and verification status

Tests live in `playground/main.cpp`. All test switches default to `1`. Set a switch to `0` in the file, or define it in the playground's preprocessor settings, then rebuild to disable that category.

| Switch | Coverage |
| --- | --- |
| `JOBKIT_TEST_GRAPH_DESTRUCTION` | Worker detection, pending graph destruction, failure and skipped work, unsubmitted graphs, and explicit error reporting. |
| `JOBKIT_TEST_WORKER_DESTRUCTION_GUARD` | Forbidden worker destruction of a submitted graph. |
| `JOBKIT_TEST_WORKER_WAIT_GUARDS` | Worker calls to pool, group, and graph waits, followed by successful worker reuse. |
| `JOBKIT_TEST_GRAPH_COMPLETIONS` | Chains of 100,000 skipped descendants, 20,000 direct dependents, concurrent joins with 1/2/4/8 workers, and 1,000 graph-lifetime stress iterations. |
| `JOBKIT_TEST_GRAPH_VALIDATION_LIFECYCLE` | Repeated validation before submission and rejection during execution and after completion. |
| `JOBKIT_TEST_GROUP_DESTRUCTION` | Pending work, task errors, worker destruction of empty/completed groups, and forbidden unfinished-group destruction. |
| `JOBKIT_TEST_SUBMISSION_FAILURE` | Forced callable-copy failures during grouped and ungrouped submission, counter rollback, exception propagation, and subsequent reuse. |

Potentially fatal or deadlocking cases run in isolated child processes with a timeout. That harness uses Windows APIs; those isolated checks are skipped on other platforms.

The latest verification passed **26 reported checks in x64 Release**. That verification disabled whole-program optimization and debug-symbol generation. Default project build settings, Debug, Win32, other compilers/platforms, and memory/race analysis have not been verified in this work. Stress checks exercise selected interleavings; they are not proof that every possible race is absent.

## Next milestones

1. Add focused shutdown-order regression coverage.
2. Verify the normal Debug and Release build configurations and perform memory/race analysis where supported.
3. Integrate one bounded CPU workload into the engine, compare its results with sequential execution, and measure scheduling overhead.

The current scheduler uses a shared FIFO task queue. Work stealing, priorities, cancellation, typed results, graph reuse, and performance tuning are future design choices rather than prerequisites for that first integration.
