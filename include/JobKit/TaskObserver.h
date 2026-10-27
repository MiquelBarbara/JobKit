#pragma once

#include <chrono>
#include <cstddef>

namespace JobKit
{
class JobGroup;
using JobClock = std::chrono::steady_clock;

// Pool-local worker index. The group is context only: never retain/dereference it
// after the callback. startedAt precedes the start hook; execution includes that
// hook's cost. Completion is delivered before group/pool completion is signalled.
struct TaskTiming
{
	std::size_t workerIndex = 0;
	const JobGroup* group = nullptr;
	JobClock::time_point queuedAt{}, startedAt{}, finishedAt{};
};

// Called concurrently by workers, outside scheduler locks. Hooks must be short,
// non-throwing, and must not wait for, submit, or destroy scheduler objects.
// A pool retains shared ownership of its observer until all workers have joined.
class TaskObserver
{
public:
	virtual ~TaskObserver() = default;
	virtual void onTaskStarted(const TaskTiming& timing) noexcept = 0;
	virtual void onTaskFinished(const TaskTiming& timing) noexcept = 0;
};
} // namespace JobKit
