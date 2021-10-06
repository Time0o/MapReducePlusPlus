#pragma once

#include <string>

namespace mr
{

template<typename K, typename V>
struct KV
{
  KV(K const &key, V const &value)
  : key(key)
  , value(value)
  {}

  K key;
  V value;
};

} // end namespace mr
