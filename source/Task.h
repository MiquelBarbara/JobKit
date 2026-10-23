#pragma once
#include <variant>
#include <functional>
#include <vector>
#include <string>

enum TaskType
{
	Stop,
	Execute
};

using Parameter = std::variant<int, float, std::string, bool>;

struct Task
{

	TaskType type;
	std::function<void(std::vector<Parameter>)> task;
	std::vector<Parameter> arguments;
};
