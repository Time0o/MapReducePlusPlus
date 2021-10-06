#pragma once

#include <coroutine>
#include <optional>
#include <utility>

namespace mr
{

template<typename T>
class Generator
{
  class Promise
  {
  public:
    Generator get_return_object()
    { return Generator{ Handle::from_promise(*this) }; }

    std::suspend_always initial_suspend() noexcept
    { return {}; }

    std::suspend_always final_suspend() noexcept
    { return {}; }

    std::suspend_always yield_value(T val) noexcept
    {
      _val = std::move(val);
      return {};
    }

    static void unhandled_exception()
    { throw; }

    std::optional<T> value() const
    { return _val; }

  private:
    std::optional<T> _val;
  };

  using Handle = std::coroutine_handle<Promise>;

public:
  using promise_type = Promise;

  explicit Generator(Handle const handle)
  : _handle(handle)
  {}

  ~Generator()
  {
    if(_handle)
      _handle.destroy();
  }

  Generator(Generator const &) = delete;
  Generator &operator=(Generator const &) = delete;
  Generator(Generator &&other) = delete;
  Generator &operator=(Generator &&other) = delete;

  T value() const
  { return *_handle.promise().value(); }

  bool next()
  {
    _handle.resume();
    return !_handle.done();
  }

private:
  Handle _handle;
};

} // end namespace mr
