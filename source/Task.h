#pragma once
#include <variant>
#include <functional>
#include <vector>
#include <string>

enum TaskType
{
	// Shutdown signal: no callable is executed for this entry.
	Stop,
	Execute
};

// Explicit runtime arguments supported by this learning implementation.
using Parameter = std::variant<int, float, std::string, bool>;

struct Task
{

	TaskType type;
	// A lambda can also carry data via captures, as dispatch does for its range.
	// Owning the callable does not extend the lifetime of objects captured by reference.
	std::function<void(std::vector<Parameter>)> task;
	// Passed by value to the callable, so executing a task currently copies this vector.
	std::vector<Parameter> arguments;
};
