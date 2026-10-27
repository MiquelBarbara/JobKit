#pragma once

#include "TaskObserver.h"
#include <cstdint>
#include <memory>
#include <vector>

namespace JobKit
{
struct WorkerMetrics
{
	std::size_t workerIndex = 0;
	std::uint64_t completedTasks = 0;
	std::chrono::nanoseconds queueWait{}, maximumQueueWait{}, execution{}, maximumExecution{};
	std::chrono::nanoseconds busy{}, capacity{};
	bool active = false;
};

// One collector per pool, constructed with that pool's worker count. Each worker
// updates its own cache-line-aligned state; no shared per-task accumulator exists.
// sample() has one controlling-thread consumer and consumes interval totals/maxima.
// Busy/capacity include partial running tasks; timings/counts belong to completions.
class MetricsObserver final : public TaskObserver
{
public:
	explicit MetricsObserver(std::size_t workers);
	~MetricsObserver();
	MetricsObserver(const MetricsObserver&) = delete;
	MetricsObserver& operator=(const MetricsObserver&) = delete;
	void onTaskStarted(const TaskTiming& timing) noexcept override;
	void onTaskFinished(const TaskTiming& timing) noexcept override;
	std::vector<WorkerMetrics> sample();
private:
	struct Worker;
	std::vector<std::unique_ptr<Worker>> _workers;
};
} // namespace JobKit
