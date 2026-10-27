# JobKit

JobKit is a standalone C++20 job system. It provides a fixed thread pool, typed futures, batched work, completion groups, and dependency graphs without engine dependencies.

Use the ownership and shutdown rules below in any host application. Memory/race analysis and application workload benchmarks remain separate validation work.

## Build and run

### CMake library and independent example

The standalone build requires CMake 3.21+, a C++20 compiler/standard library with
`std::jthread`, and platform thread support. It builds only the static library;
the example is a separate application project. There are no engine dependencies,
downloads, playground targets or test targets in this build.

From the JobKit repository root:

```sh
cmake -S . -B out/cmake -DCMAKE_BUILD_TYPE=Release
cmake --build out/cmake --config Release
```

For a multi-configuration generator (Visual Studio or Ninja Multi-Config), select
Debug/Release with `--config`; `CMAKE_BUILD_TYPE` applies to single-configuration
generators such as Ninja and Unix Makefiles. On Windows, use a CMake version
supporting your selected Visual Studio generator, or use Ninja in a Developer
PowerShell. For example, current Visual Studio supports
`-G "Visual Studio 18 2026" -A x64` with its bundled CMake.

Consumers add the checkout as a subdirectory and link the namespaced target:

```cmake
add_subdirectory(external/JobKit)
add_executable(MyApp main.cpp)
target_link_libraries(MyApp PRIVATE JobKit::JobKit)
```

The target propagates its public include directory, C++20 requirement and
`Threads::Threads` linkage. JobKit does not override the host's compiler flags,
language extensions, position-independent-code policy or MSVC runtime choice.
For an MSVC build, match platform, configuration and `CMAKE_MSVC_RUNTIME_LIBRARY`
between the library and consumers. The CMake default uses the DLL runtime;
select a static runtime consistently for both sides if your host requires it.

To build the independent example directly from sources (run at the JobKit root):

```sh
cmake -S examples/basic -B out/example-source -DJOBKIT_SOURCE_DIR="$PWD" -DCMAKE_BUILD_TYPE=Release
cmake --build out/example-source --config Release
```

Run `out/example-source/JobKitExample` on single-configuration Unix builds,
`out/example-source/JobKitExample.exe` with Windows Ninja, or
`out/example-source/Release/JobKitExample.exe` with Visual Studio. It prints
`Sum of squares 1..256: 5625216`, using grouped dispatch followed by a typed future
with a move-only capture/result. All waits occur on the controlling thread.

For an installed package, standalone builds enable `JOBKIT_INSTALL` by default:

```sh
cmake --install out/cmake --config Release --prefix "$PWD/out/install"
cmake -S examples/basic -B out/example-installed -DCMAKE_PREFIX_PATH="$PWD/out/install" -DCMAKE_BUILD_TYPE=Release
cmake --build out/example-installed --config Release
```

The example uses `find_package(JobKit CONFIG REQUIRED)` when `JOBKIT_SOURCE_DIR`
is empty. Another installed-package consumer needs just:

```cmake
find_package(JobKit CONFIG REQUIRED)
target_link_libraries(MyApp PRIVATE JobKit::JobKit)
```

Packages export `JobKit::JobKit` and resolve their thread dependency without source
checkout paths. Standard GNU installation directories can be customized through
`CMAKE_INSTALL_INCLUDEDIR` and `CMAKE_INSTALL_LIBDIR`; `CMAKE_PREFIX_PATH` locates
standard installation layouts. With a nonstandard library directory, set
`JobKit_DIR` to `<prefix>/<library-directory>/cmake/JobKit` directly. Debug archives
have a `d` suffix, allowing Debug and Release
to be installed into the same prefix. Build and install each desired configuration
before configuring a consumer. Match installed configurations and MSVC runtimes
with the consumer; a Release-only archive is not a Debug-runtime substitute.
For installed packages, RelWithDebInfo/MinSizeRel consumers use their matching
archive if installed, otherwise Release. Explicit consumer configuration mappings
are respected; optimized consumers never implicitly choose Debug as that default.

When embedded with `add_subdirectory`/FetchContent, `JOBKIT_INSTALL` defaults to
OFF so the dependency does not add install rules to the host unless requested.
The example always builds separately, even if installation is enabled.

### Visual Studio projects

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
cmake/           Installed-package configuration
examples/basic/  Independent CMake consumer application
playground/      Console test application
out/             Build outputs and intermediate files
```

## Public API

All library types belong to the `JobKit` namespace.

| Type | Purpose |
| --- | --- |
| `ThreadPool` | Owns a fixed number of workers; provides `push()`, `submit()`, `dispatch()`, and `wait()`. Construction requires at least one worker. |
| `Task` | Holds a `std::function<void()>` and an optional non-owning `JobGroup*`. |
| `JobGroup` | Tracks completion of a selected set of tasks independently of other pool work. |
| `JobGraph` | Owns jobs, validates dependencies, and submits a graph once. |
| `JobNode` | Represents a graph job. `child.dependsOn(parent)` makes the child wait for the parent. |
| `ThreadQueue<T>` | Provides synchronized FIFO storage; closing it rejects pushes and lets consumers drain existing entries. |

`Task::task` has the signature `void()`. Supply input through captures of your own types. The former `Parameter` variant and argument vector have been removed; update existing task callbacks to take no arguments and capture their inputs instead. Direct tasks use copyable callable storage; `submit()` also accepts move-only callables.

`push()` returns `true` when submission succeeds and throws if submission fails. A rejected task does not remain counted as unfinished work.

## Typed results

`submit(callable)` accepts a zero-argument callable and returns `std::future<Result>`, including `std::future<void>`. The callable is owned by a shared `std::packaged_task`, allowing move-only captures and results without changing the queue's copyable task storage.

```cpp
#include <JobKit/ThreadPool.h>
#include <chrono>
#include <memory>

int main()
{
    JobKit::ThreadPool pool{2};
    auto result = pool.submit([input = std::make_unique<int>(21)] {
        return std::make_unique<int>(*input * 2);
    });
    // Hosts can poll with wait_for(std::chrono::seconds(0)) before publishing.
    auto output = result.get();
    return *output == 42 ? 0 : 1;
}
```

Callable failures are stored only in the returned future and rethrown by `get()`; they are not also reported by `pool.wait()`. Submission failures throw from `submit()` before returning a future. Typed submissions do not accept a group. Use direct tasks or graphs for group/graph error reporting.

Discarding this future does not cancel or wait for the task. Retain captured data until execution finishes, and consume the future when its result or error matters. `pool.wait()` and pool destruction still finish accepted typed submissions, including those whose futures were discarded. Never block on a future from a JobKit worker: standard futures do not implement JobKit's worker-wait guard.

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
        [&input]() {
            input = {1, 2, 3, 4};
        }});

    auto& transform = graph.createJob({
        [&input, &output]() {
            for (std::size_t i = 0; i < input.size(); ++i)
                output[i] = input[i] * 2;
        }});

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

## Optional scheduling observation

Construct a pool with a `std::shared_ptr<TaskObserver>` to receive paired worker
start/finish callbacks. Without an observer there are no timing clock reads or
callbacks. `setObserverEnabled(false)` skips timing and callbacks for subsequent
submissions, while entries already marked for observation finish their hook pair.
There is still a queued timing envelope and a flag check; disabled instrumentation
is not a zero-cost guarantee. Observation is enabled initially when supplied.

`TaskTiming` contains a pool-local worker index, the optional non-owning group
pointer, and steady-clock enqueue/start/finish timestamps. Enqueue time is captured
after copying the callable and before queue insertion. Execution includes the start
hook, but excludes the finish hook and completion bookkeeping. Typed future errors
are still delivered only by their futures; hooks also run for failed tasks and graph
wrappers. A queued graph wrapper can do graph completion/descendant submission work,
so its measured duration is not necessarily only one user node callback.

Hooks run concurrently on workers, outside scheduler locks, before group/pool
completion. They must be short and non-throwing, and must not submit, wait for or
destroy scheduler objects. Never retain/dereference the supplied group pointer
after its callback. The pool owns its observer through worker join; an observer
must not own its pool (which would form a cycle). Existing worker-wait and graph
lifetime rules also apply when observation is enabled.

The optional reusable collector has no engine dependency:

```cpp
#include <JobKit/MetricsObserver.h>
#include <JobKit/ThreadPool.h>

auto metrics = std::make_shared<JobKit::MetricsObserver>(4);
JobKit::ThreadPool pool(4, metrics); // One collector per pool, matching worker count.
metrics->sample();                 // Discard the initial interval if desired.
pool.dispatch(1000, 64, [](std::size_t begin, std::size_t end) {
    for (auto i = begin; i < end; ++i) { /* independent work */ }
});
pool.wait();
auto interval = metrics->sample(); // One controlling-thread consumer.
auto unfinished = pool.unfinishedTaskCount(); // Queued + executing; not an idle barrier.
```

Each `WorkerMetrics` provides completed-task count, summed and maximum queue wait
and execution durations, busy/available worker time, and a live active flag.
`sample()` consumes interval counts/sums/maxima, using a short lock per worker.
Workers update independent, cache-line-aligned state rather than a shared global
accumulator. Busy time includes partial tasks across samples, from registration in
the start hook to registration in the finish hook; queue/execution durations and
counts are attributed to the completion interval. Mean wait/run is summed duration
divided by completed count; utilization is summed busy divided by summed capacity.
Do not average per-worker percentages or treat parallel duration sums as frame time.

Sampling workers and the outstanding gauge is not atomic across the pool. A brief
queue spike between samples can be missed. Jobs accepted while observation is off
remain unmeasured, including when observation is later re-enabled. To start a fresh
display interval, discard one `sample()` on re-enable; already observed running
jobs still contribute subsequent partial busy time and eventually their full
completion durations. Avoid changing scheduler policy until application captures
and observer overhead have been measured.

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

The callable/future API revision passed **26 reported checks in both x64 Debug and x64 Release**, using the normal project settings (including Release whole-program optimization and debug-symbol generation). A temporary console exercise also verified typed values, `void`, references, move-only captures/results, future exceptions, discarded futures, and accepted work draining during pool destruction. Win32, other compilers/platforms, and memory/race analysis remain unverified. Stress checks exercise selected interleavings; they are not proof that every possible race is absent.

The scheduling-observation revision also passed the normal Debug/Release x64
builds and all 26 existing playground checks. A temporary console exercise
verified partial busy intervals, queued/executing gauges, toggling with pending
work, dispatch counts, direct/group/future/graph failures, skipped descendants,
and observed work draining during destruction. No additional test suite/target
was introduced.

Three Release measurements of 20,000 empty dispatch batches on four workers
produced median round times of 5.80--6.29 ms without an observer, 5.91--6.01 ms
with its observer disabled, and 6.83--7.38 ms enabled. For 32 independent CPU
tasks of roughly 25 microseconds each, medians were 0.203--0.207 ms without
observation, 0.204--0.207 ms disabled and 0.205--0.207 ms enabled. These synthetic
results show why observation cost matters for tiny jobs; they do not establish
an application-wide speedup or a zero-overhead disabled path.

The standalone CMake path was built and its independent source/installed-package
examples run manually in Debug and Release with MSVC 19.51 (Visual Studio x64,
CMake 4.3.1) and Ubuntu GCC 13.3/libstdc++ (Unix Makefiles, CMake 3.31.10). Each
printed `Sum of squares 1..256: 5625216`. Relocated packages, paths with spaces,
custom GNU install directories, a Windows Ninja/static-Debug-runtime build, and
an installed RelWithDebInfo consumer using the Release archive were exercised.
The standalone build exposes only the library; embedding defaults installation
off, and the example consumes C++20/includes/thread linkage from `JobKit::JobKit`.
Other compilers, macOS, other architectures, and memory/race analysis remain
unverified. This build milestone changes no scheduler implementation or APIs.

## Next milestones

1. Measure application workloads and scheduling overhead before changing queue behavior or task storage.
2. Extend standalone CMake validation to other toolchains/platforms and perform memory/race analysis where supported.
3. Explore cooperative cancellation, graph adoption/reuse, and optional diagnostics using generic application requirements.

The current scheduler uses a shared FIFO task queue. Work stealing, priorities, cancellation, graph reuse, and performance tuning are future design choices rather than prerequisites for that first integration.
