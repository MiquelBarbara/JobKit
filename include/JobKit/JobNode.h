#pragma once
#include "Task.h"
#include <atomic>
#include <vector>
#include <cstddef>

namespace JobKit
{

class JobGraph;

enum class JobState
{
	Pending,
	Running,
	Succeeded,
	Failed,
	Skipped
};

class JobNode
{
public:
	friend class JobGraph;

	explicit JobNode(Task task);
	void dependsOn(JobNode& prerequisite);

private:
	Task task;
	bool _sealed = false;
	std::atomic<JobState> state{ JobState::Pending };
	std::atomic<bool> failedPrerequisite{ false };
	std::atomic<std::size_t> prerequisiteCounter{ 0 };
	std::vector<JobNode*> dependents;
};

} // namespace JobKit
