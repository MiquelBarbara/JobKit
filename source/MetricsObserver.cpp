#include <JobKit/MetricsObserver.h>
#include <algorithm>
#include <mutex>

namespace JobKit
{
struct alignas(64) MetricsObserver::Worker
{
	std::mutex mutex;
	WorkerMetrics interval;
	JobClock::time_point activeSince{}, sampledAt = JobClock::now();
	std::chrono::nanoseconds finishedBusy{}, sampledBusy{};
};

MetricsObserver::MetricsObserver(std::size_t workers)
{
	_workers.reserve(workers);
	for (std::size_t i = 0; i < workers; ++i)
	{
		auto worker = std::make_unique<Worker>();
		worker->interval.workerIndex = i;
		_workers.push_back(std::move(worker));
	}
}
MetricsObserver::~MetricsObserver() = default;

void MetricsObserver::onTaskStarted(const TaskTiming& timing) noexcept
{
	if (timing.workerIndex >= _workers.size()) return;
	auto& worker = *_workers[timing.workerIndex];
	std::lock_guard lock(worker.mutex);
	// Occupancy starts under the same lock as sampling, so a racing sample
	// cannot count work retroactively before its interval boundary.
	worker.activeSince = JobClock::now();
	worker.interval.active = true;
}
void MetricsObserver::onTaskFinished(const TaskTiming& timing) noexcept
{
	if (timing.workerIndex >= _workers.size()) return;
	auto& worker = *_workers[timing.workerIndex];
	std::lock_guard lock(worker.mutex);
	auto& interval = worker.interval;
	const auto queued = std::chrono::duration_cast<std::chrono::nanoseconds>(timing.startedAt - timing.queuedAt);
	const auto executed = std::chrono::duration_cast<std::chrono::nanoseconds>(timing.finishedAt - timing.startedAt);
	++interval.completedTasks;
	interval.queueWait += queued;
	interval.execution += executed;
	interval.maximumQueueWait = std::max(interval.maximumQueueWait, queued);
	interval.maximumExecution = std::max(interval.maximumExecution, executed);
	// Include observation overhead in worker occupancy, keeping live busy totals
	// monotonic even when a sample races the completion hook's lock acquisition.
	worker.finishedBusy += std::chrono::duration_cast<std::chrono::nanoseconds>(JobClock::now() - worker.activeSince);
	interval.active = false;
}
std::vector<WorkerMetrics> MetricsObserver::sample()
{
	std::vector<WorkerMetrics> result;
	result.reserve(_workers.size());
	for (auto& state : _workers)
	{
		auto& worker = *state;
		std::lock_guard lock(worker.mutex);
		const auto now = JobClock::now();
		const auto busy = worker.finishedBusy + (worker.interval.active
			? std::chrono::duration_cast<std::chrono::nanoseconds>(now - worker.activeSince)
			: std::chrono::nanoseconds{});
		auto sample = worker.interval;
		sample.busy = busy - worker.sampledBusy;
		sample.capacity = std::chrono::duration_cast<std::chrono::nanoseconds>(now - worker.sampledAt);
		result.push_back(sample);
		worker.sampledBusy = busy;
		worker.sampledAt = now;
		worker.interval = {};
		worker.interval.workerIndex = sample.workerIndex;
		worker.interval.active = sample.active;
	}
	return result;
}
} // namespace JobKit
