#pragma once

#include <string>

namespace mr
{

template<typename K, typename V>
struct KeyValue
{
  using key_type = K;
  using value_type = V;

  KeyValue(K const &key, V const &value)
  : key(key)
  , value(value)
  {}

  K key;
  V value;
};

} // end namespace mr
