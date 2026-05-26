#pragma once

#include "doof_runtime.hpp"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace doof_event {

class NativeAsyncEventChannel;

namespace detail {

class MainEventDispatcher {
public:
    static MainEventDispatcher& shared() {
        static MainEventDispatcher dispatcher;
        return dispatcher;
    }

    void registerChannel(bool keepsAlive) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (keepsAlive) {
            ++keepAliveCount_;
        }
    }

    int32_t trySend(
        const std::shared_ptr<NativeAsyncEventChannel>& channel,
        doof::callback<void()> task
    );

    bool tryClose(NativeAsyncEventChannel& channel);

    bool waitAndDispatchOne();

private:
    MainEventDispatcher() = default;

    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::shared_ptr<NativeAsyncEventChannel>> readyChannels_;
    int64_t keepAliveCount_ = 0;
};

}  // namespace detail

class NativeAsyncEventChannel : public std::enable_shared_from_this<NativeAsyncEventChannel> {
public:
    static std::shared_ptr<NativeAsyncEventChannel> create(int32_t capacity, bool keepsAlive) {
        if (capacity <= 0) {
            doof::panic("event channel capacity must be positive");
        }
        return std::shared_ptr<NativeAsyncEventChannel>(
            new NativeAsyncEventChannel(capacity, keepsAlive)
        );
    }

    ~NativeAsyncEventChannel() {
        (void)tryClose();
    }

    int32_t trySend(doof::callback<void()> task) {
        return detail::MainEventDispatcher::shared().trySend(shared_from_this(), std::move(task));
    }

    bool tryClose() {
        return detail::MainEventDispatcher::shared().tryClose(*this);
    }

private:
    friend class detail::MainEventDispatcher;

    explicit NativeAsyncEventChannel(int32_t capacity, bool keepsAlive)
        : capacity_(capacity), keepsAlive_(keepsAlive) {
        detail::MainEventDispatcher::shared().registerChannel(keepsAlive_);
    }

    int32_t capacity_;
    bool keepsAlive_;
    bool closed_ = false;
    bool scheduled_ = false;
    std::deque<doof::callback<void()>> tasks_;
};

inline int32_t detail::MainEventDispatcher::trySend(
    const std::shared_ptr<NativeAsyncEventChannel>& channel,
    doof::callback<void()> task
) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel->closed_) {
            return 2;  // Closed
        }
        if (static_cast<int64_t>(channel->tasks_.size()) >= static_cast<int64_t>(channel->capacity_)) {
            return 1;  // Full
        }

        channel->tasks_.push_back(std::move(task));
        if (!channel->scheduled_) {
            channel->scheduled_ = true;
            readyChannels_.push_back(channel);
        }
    }
    ready_.notify_one();
    return 0;  // Accepted
}

inline bool detail::MainEventDispatcher::tryClose(NativeAsyncEventChannel& channel) {
    bool removedKeepAlive = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel.closed_) {
            return false;
        }

        channel.closed_ = true;
        removedKeepAlive = channel.keepsAlive_;
        if (removedKeepAlive && keepAliveCount_ > 0) {
            --keepAliveCount_;
        }
    }

    if (removedKeepAlive) {
        ready_.notify_all();
    }
    return true;
}

inline bool detail::MainEventDispatcher::waitAndDispatchOne() {
    doof::callback<void()> task;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [this] {
            return !readyChannels_.empty() || keepAliveCount_ == 0;
        });

        if (readyChannels_.empty()) {
            return false;
        }

        auto channel = std::move(readyChannels_.front());
        readyChannels_.pop_front();
        channel->scheduled_ = false;

        task = std::move(channel->tasks_.front());
        channel->tasks_.pop_front();

        if (!channel->tasks_.empty()) {
            channel->scheduled_ = true;
            readyChannels_.push_back(std::move(channel));
        }
    }

    task.call();
    return true;
}

inline void runMainEventLoop() {
    while (detail::MainEventDispatcher::shared().waitAndDispatchOne()) {
    }
}

}  // namespace doof_event
