#pragma once

#include <cctype>
#include <numeric>
#include <sstream>
#include <string>

#include "generator.h"
#include "key_value.h"

namespace mr
{

using KV = KeyValue<std::string, int>;

Generator<KV> map(std::string const &file, std::stringstream const &ss)
{
  auto str { ss.str() };

  std::string::iterator beg, end;

  for (beg = std::find_if(str.begin(), str.end(), ::isalpha);
       beg != str.end();
       beg = std::find_if(end, str.end(), ::isalpha)) {

    end = std::find_if_not(beg, str.end(), ::isalpha);

    std::string word(beg, end);

    co_yield {word, 1};
  }
}

template<typename IT>
KV::value_type reduce(KV::key_type key, IT values_first, IT values_last)
{
  return std::accumulate(values_first, values_last, 0);
}

} // end namespace mr
