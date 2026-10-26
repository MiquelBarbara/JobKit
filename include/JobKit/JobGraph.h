#pragma once
#include "JobNode.h"
#include "JobGroup.h"
#include <vector>
#include <memory>
#include <unordered_map>
#include <cstddef>
#include <exception>

namespace JobKit
{

class ThreadPool;

class JobGraph
{
private:
	struct ValidationState
	{
		std::unordered_map<const JobNode*, std::size_t> remaining;
		std::vector<const JobNode*> readyNodes;
	};

	ValidationState buildValidationState() const;
	void validateLinks(const std::unordered_map<const JobNode*, std::size_t>& remaining) const;
	void validateAcyclic(ValidationState& state) const;
	std::vector<JobNode*> prepareSubmission();
	void executeNode(ThreadPool& pool, JobNode& node);
	void finishNode(ThreadPool& pool, JobNode& node,
		JobState result, std::exception_ptr error = nullptr);
	void scheduleNode(ThreadPool& pool, JobNode& node);

public:
	JobNode& createJob(Task task);
	void submit(ThreadPool& pool);
	void wait();
	void validate() const;

private:
	bool _submitted = false;
	JobGroup _completion;
	std::vector<std::unique_ptr<JobNode>> _nodes;
};

} // namespace JobKit
