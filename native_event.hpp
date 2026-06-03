#pragma once

#include "doof_runtime.hpp"

#include <condition_variable>
#include <cstdint>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace doof_event {

class NativeChannel;
class NativeTimer;

namespace detail {

enum class TimerKind {
    Timeout,
    Interval,
};

enum class TimerState {
    Scheduled,
    Dispatching,
    Canceled,
    Completed,
};

class MainEventDispatcher {
public:
    static MainEventDispatcher& shared() {
        static MainEventDispatcher dispatcher;
        return dispatcher;
    }

    void addKeepAliveSource(bool keepsAlive);

    int32_t trySendMessage(
        const std::shared_ptr<NativeChannel>& channel,
        doof::callback<void()> task,
        bool hasKey,
        const std::string& key
    );

    bool tryClose(NativeChannel& channel);

    void startTimer(const std::shared_ptr<NativeTimer>& timer);

    bool cancelTimer(NativeTimer& timer);

    void commitTimer(const std::shared_ptr<NativeTimer>& timer);

    void finishTimerTick(NativeTimer& timer);

    int32_t drainReady();

    bool waitAndDispatchOne();

    void setWakeHandler(std::function<void()> handler);

private:
    MainEventDispatcher() = default;

    void removeKeepAliveSourceLocked(bool keepsAlive);
    bool takeReadyTaskLocked(doof::callback<void()>& task);
    void notifyReady();

    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::shared_ptr<NativeChannel>> readyChannels_;
    std::deque<doof::callback<void()>> readyTasks_;
    int64_t keepAliveCount_ = 0;
    std::function<void()> wakeHandler_;
};

}  // namespace detail

class NativeChannel : public std::enable_shared_from_this<NativeChannel> {
public:
    static std::shared_ptr<NativeChannel> createChannel(
        int32_t capacity,
        int32_t highWater,
        int32_t lowWater,
        bool keepsAlive,
        doof::callback<void()> readyHandler,
        doof::callback<void()> closedHandler
    ) {
        if (capacity <= 0) {
            doof::panic("Channel capacity must be positive");
        }
        if (highWater <= 0 || highWater > capacity) {
            doof::panic("Channel highWater must be between 1 and capacity");
        }
        if (lowWater < 0 || lowWater > highWater) {
            doof::panic("Channel lowWater must be between 0 and highWater");
        }

        return std::shared_ptr<NativeChannel>(
            new NativeChannel(
                capacity,
                highWater,
                lowWater,
                keepsAlive,
                std::move(readyHandler),
                std::move(closedHandler)
            )
        );
    }

    ~NativeChannel() {
        (void)tryClose();
    }

    int32_t trySendMessage(doof::callback<void()> task, bool hasKey, const std::string& key) {
        return detail::MainEventDispatcher::shared().trySendMessage(
            shared_from_this(),
            std::move(task),
            hasKey,
            key
        );
    }

    bool tryClose() {
        return detail::MainEventDispatcher::shared().tryClose(*this);
    }

private:
    friend class detail::MainEventDispatcher;

    NativeChannel(
        int32_t capacity,
        int32_t highWater,
        int32_t lowWater,
        bool keepsAlive,
        doof::callback<void()> readyHandler,
        doof::callback<void()> closedHandler
    ) : capacity_(capacity),
        highWater_(highWater),
        lowWater_(lowWater),
        keepsAlive_(keepsAlive),
        readyHandler_(std::move(readyHandler)),
        closedHandler_(std::move(closedHandler)),
        sendsReadyAndClosed_(true) {
        detail::MainEventDispatcher::shared().addKeepAliveSource(keepsAlive_);
    }

    enum class TaskKind {
        Message,
        Ready,
        Closed,
    };

    struct QueuedTask {
        TaskKind kind;
        bool hasKey;
        std::string key;
        doof::callback<void()> task;
    };

    int32_t capacity_;
    int32_t highWater_ = 0;
    int32_t lowWater_ = 0;
    bool keepsAlive_;
    bool closed_ = false;
    bool scheduled_ = false;
    bool sendsReadyAndClosed_ = false;
    bool waitingForReady_ = false;
    bool closedQueued_ = false;
    int32_t messageCount_ = 0;
    doof::callback<void()> readyHandler_;
    doof::callback<void()> closedHandler_;
    std::deque<QueuedTask> tasks_;
};

class NativeTimer : public std::enable_shared_from_this<NativeTimer> {
public:
    static std::shared_ptr<NativeTimer> createTimeout(
        int64_t delayNanos,
        bool keepsAlive,
        doof::callback<void()> handler
    ) {
        if (delayNanos < 0) {
            doof::panic("setTimeout delay must not be negative");
        }

        auto timer = std::shared_ptr<NativeTimer>(
            new NativeTimer(delayNanos, detail::TimerKind::Timeout, keepsAlive, std::move(handler))
        );
        detail::MainEventDispatcher::shared().startTimer(timer);
        return timer;
    }

    static std::shared_ptr<NativeTimer> createInterval(
        int64_t intervalNanos,
        bool keepsAlive,
        doof::callback<void()> handler
    ) {
        if (intervalNanos <= 0) {
            doof::panic("setInterval interval must be positive");
        }

        auto timer = std::shared_ptr<NativeTimer>(
            new NativeTimer(intervalNanos, detail::TimerKind::Interval, keepsAlive, std::move(handler))
        );
        detail::MainEventDispatcher::shared().startTimer(timer);
        return timer;
    }

    bool cancel() {
        return detail::MainEventDispatcher::shared().cancelTimer(*this);
    }

private:
    friend class detail::MainEventDispatcher;

    NativeTimer(
        int64_t periodNanos,
        detail::TimerKind kind,
        bool keepsAlive,
        doof::callback<void()> handler
    ) : periodNanos_(periodNanos),
        kind_(kind),
        keepsAlive_(keepsAlive),
        handler_(std::move(handler)) {
    }

    int64_t periodNanos_;
    detail::TimerKind kind_;
    bool keepsAlive_;
    bool countedKeepAlive_ = false;
    detail::TimerState state_ = detail::TimerState::Scheduled;
    doof::callback<void()> handler_;
};

inline void detail::MainEventDispatcher::addKeepAliveSource(bool keepsAlive) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (keepsAlive) {
        ++keepAliveCount_;
    }
}

inline void detail::MainEventDispatcher::removeKeepAliveSourceLocked(bool keepsAlive) {
    if (keepsAlive && keepAliveCount_ > 0) {
        --keepAliveCount_;
    }
}

inline int32_t detail::MainEventDispatcher::trySendMessage(
    const std::shared_ptr<NativeChannel>& channel,
    doof::callback<void()> task,
    bool hasKey,
    const std::string& key
) {
    bool shouldNotify = false;
    int32_t code = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel->closed_) {
            return 3;  // Closed
        }

        if (hasKey) {
            for (auto& queued : channel->tasks_) {
                if (
                    queued.kind == NativeChannel::TaskKind::Message &&
                    queued.hasKey &&
                    queued.key == key
                ) {
                    queued.task = std::move(task);
                    if (channel->messageCount_ >= channel->highWater_) {
                        channel->waitingForReady_ = true;
                        return 1;  // Accepted, high backpressure
                    }
                    return 0;  // Accepted
                }
            }
        }

        if (channel->messageCount_ >= channel->capacity_) {
            return 2;  // Full
        }

        channel->tasks_.push_back(NativeChannel::QueuedTask {
            NativeChannel::TaskKind::Message,
            hasKey,
            key,
            std::move(task),
        });
        ++channel->messageCount_;

        if (channel->messageCount_ >= channel->highWater_) {
            channel->waitingForReady_ = true;
            code = 1;  // Accepted, high backpressure
        }

        if (!channel->scheduled_) {
            channel->scheduled_ = true;
            readyChannels_.push_back(channel);
            shouldNotify = true;
        }
    }

    if (shouldNotify) {
        notifyReady();
    }
    return code;
}

inline bool detail::MainEventDispatcher::tryClose(NativeChannel& channel) {
    bool removedKeepAlive = false;
    bool shouldNotify = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel.closed_) {
            return false;
        }

        channel.closed_ = true;
        removedKeepAlive = channel.keepsAlive_;
        removeKeepAliveSourceLocked(removedKeepAlive);

        if (channel.sendsReadyAndClosed_ && !channel.closedQueued_) {
            auto self = channel.weak_from_this().lock();
            if (self) {
                channel.closedQueued_ = true;
                channel.tasks_.push_back(NativeChannel::QueuedTask {
                    NativeChannel::TaskKind::Closed,
                    false,
                    std::string(),
                    std::move(channel.closedHandler_),
                });
                if (!channel.scheduled_) {
                    channel.scheduled_ = true;
                    readyChannels_.push_back(std::move(self));
                    shouldNotify = true;
                }
            }
        }
    }

    if (removedKeepAlive || shouldNotify) {
        notifyReady();
    }
    return true;
}

inline void detail::MainEventDispatcher::startTimer(const std::shared_ptr<NativeTimer>& timer) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timer->state_ != TimerState::Scheduled) {
            return;
        }
        if (timer->keepsAlive_ && !timer->countedKeepAlive_) {
            timer->countedKeepAlive_ = true;
            ++keepAliveCount_;
        }
    }

    std::thread([timer] {
        auto delay = std::chrono::nanoseconds(timer->periodNanos_);
        if (delay.count() > 0) {
            std::this_thread::sleep_for(delay);
        }
        detail::MainEventDispatcher::shared().commitTimer(timer);
    }).detach();
}

inline bool detail::MainEventDispatcher::cancelTimer(NativeTimer& timer) {
    bool removedKeepAlive = false;
    bool canceled = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timer.state_ == TimerState::Scheduled) {
            timer.state_ = TimerState::Canceled;
            canceled = true;
        } else if (
            timer.kind_ == TimerKind::Interval &&
            timer.state_ == TimerState::Dispatching
        ) {
            timer.state_ = TimerState::Canceled;
            canceled = true;
        }

        if (canceled && timer.countedKeepAlive_) {
            timer.countedKeepAlive_ = false;
            removedKeepAlive = true;
            removeKeepAliveSourceLocked(true);
        }
    }

    if (removedKeepAlive) {
        notifyReady();
    }
    return canceled;
}

inline void detail::MainEventDispatcher::commitTimer(const std::shared_ptr<NativeTimer>& timer) {
    bool shouldNotify = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timer->state_ != TimerState::Scheduled) {
            return;
        }

        timer->state_ = TimerState::Dispatching;
        if (timer->kind_ == TimerKind::Timeout && timer->countedKeepAlive_) {
            timer->countedKeepAlive_ = false;
            removeKeepAliveSourceLocked(true);
        }

        readyTasks_.push_back([timer] {
            timer->handler_.call();
            detail::MainEventDispatcher::shared().finishTimerTick(*timer);
        });
        shouldNotify = true;
    }

    if (shouldNotify) {
        notifyReady();
    }
}

inline void detail::MainEventDispatcher::finishTimerTick(NativeTimer& timer) {
    if (timer.kind_ == TimerKind::Timeout) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timer.state_ == TimerState::Dispatching) {
            timer.state_ = TimerState::Completed;
        }
        return;
    }

    bool shouldRestart = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timer.state_ == TimerState::Dispatching) {
            timer.state_ = TimerState::Scheduled;
            shouldRestart = true;
        }
    }

    if (shouldRestart) {
        startTimer(timer.shared_from_this());
    }
}

inline bool detail::MainEventDispatcher::takeReadyTaskLocked(doof::callback<void()>& task) {
    if (!readyTasks_.empty()) {
        task = std::move(readyTasks_.front());
        readyTasks_.pop_front();
        return true;
    }

    if (!readyChannels_.empty()) {
        auto channel = std::move(readyChannels_.front());
        readyChannels_.pop_front();
        channel->scheduled_ = false;

        auto queued = std::move(channel->tasks_.front());
        channel->tasks_.pop_front();
        task = std::move(queued.task);

        if (queued.kind == NativeChannel::TaskKind::Message) {
            --channel->messageCount_;
            if (
                channel->waitingForReady_ &&
                channel->messageCount_ <= channel->lowWater_
            ) {
                channel->waitingForReady_ = false;
                channel->tasks_.push_front(NativeChannel::QueuedTask {
                    NativeChannel::TaskKind::Ready,
                    false,
                    std::string(),
                    channel->readyHandler_,
                });
            }
        }

        if (!channel->tasks_.empty()) {
            channel->scheduled_ = true;
            readyChannels_.push_back(std::move(channel));
        }
        return true;
    }

    return false;
}

inline int32_t detail::MainEventDispatcher::drainReady() {
    int32_t dispatched = 0;
    while (true) {
        doof::callback<void()> task;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!takeReadyTaskLocked(task)) {
                return dispatched;
            }
        }

        task.call();
        ++dispatched;
    }
}

inline bool detail::MainEventDispatcher::waitAndDispatchOne() {
    doof::callback<void()> task;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [this] {
            return !readyTasks_.empty() || !readyChannels_.empty() || keepAliveCount_ == 0;
        });

        if (!takeReadyTaskLocked(task)) {
            return false;
        }
    }

    task.call();
    return true;
}

inline void detail::MainEventDispatcher::setWakeHandler(std::function<void()> handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    wakeHandler_ = std::move(handler);
}

inline void detail::MainEventDispatcher::notifyReady() {
    std::function<void()> wakeHandler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        wakeHandler = wakeHandler_;
    }

    ready_.notify_all();
    if (wakeHandler) {
        wakeHandler();
    }
}

inline void runMainEventLoop() {
    while (detail::MainEventDispatcher::shared().waitAndDispatchOne()) {
    }
}

inline int32_t drainMainEventLoop() {
    return detail::MainEventDispatcher::shared().drainReady();
}

inline void setMainEventWakeHandler(std::function<void()> handler) {
    detail::MainEventDispatcher::shared().setWakeHandler(std::move(handler));
}

inline void setMainEventWakeCallback(doof::callback<void()> handler) {
    detail::MainEventDispatcher::shared().setWakeHandler([handler]() mutable {
        handler.call();
    });
}

inline void clearMainEventWakeHandler() {
    detail::MainEventDispatcher::shared().setWakeHandler(nullptr);
}

}  // namespace doof_event
