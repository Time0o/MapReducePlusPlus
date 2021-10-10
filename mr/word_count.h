#pragma once

#include <coroutine>
#include <fstream>
#include <string>

#include "generator.h"
#include "key_value.h"

namespace mr
{

Generator<KV<std::string, int>>
map(std::string const &file, std::ifstream &content)
{
  std::string word;
  while (content >> word)
    co_yield {word, 1};
}

} // end namespace mr
