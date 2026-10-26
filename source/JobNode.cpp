#include <JobKit/JobNode.h>
#include <stdexcept>
#include <utility>

namespace JobKit
{

JobNode::JobNode(Task task)
    : task(std::move(task))
{
}

void JobNode::dependsOn(JobNode& prerequisite)
{
	if (_sealed || prerequisite._sealed)
	{
		throw std::logic_error("Cannot add dependencies after submission");
	}

	if (&prerequisite == this)
	{
		throw std::invalid_argument("A job cannot depend on itself");
	}

	prerequisite.dependents.push_back(this);
	prerequisiteCounter++;
}

} // namespace JobKit
