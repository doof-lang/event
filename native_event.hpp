#pragma once

#include "doof_runtime.hpp"

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

constexpr int32_t kActorChannelBatchSize = 32;

inline doof::detail::CallbackDomain* currentOrApplicationDomain() {
    auto* owner = doof::current_actor_domain();
    if (owner) {
        return owner;
    }
    return &doof::detail::ApplicationDomain::shared();
}

inline bool isApplicationDomain(doof::detail::CallbackDomain* owner) {
    return doof::detail::ApplicationDomain::is_application_domain(owner);
}

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

    void drainChannel(const std::shared_ptr<NativeChannel>& channel);

    int32_t drainReady();

    bool waitAndDispatchOne();

    void setWakeHandler(std::function<void()> handler);

private:
    MainEventDispatcher() = default;

    void removeKeepAliveSourceLocked(bool keepsAlive);
    bool scheduleChannelLocked(const std::shared_ptr<NativeChannel>& channel);
    void scheduleOwnerChannel(const std::shared_ptr<NativeChannel>& channel);
    bool takeChannelTaskLocked(
        const std::shared_ptr<NativeChannel>& channel,
        doof::callback<void()>& task
    );

    std::mutex mutex_;
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
        owner_(detail::currentOrApplicationDomain()),
        keepsAlive_(keepsAlive),
        readyHandler_(std::move(readyHandler)),
        closedHandler_(std::move(closedHandler)),
        sendsReadyAndClosed_(true) {
        detail::MainEventDispatcher::shared().addKeepAliveSource(
            detail::isApplicationDomain(owner_) && keepsAlive_
        );
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
    doof::detail::CallbackDomain* owner_ = nullptr;
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
        owner_(detail::currentOrApplicationDomain()),
        handler_(std::move(handler)) {
    }

    int64_t periodNanos_;
    detail::TimerKind kind_;
    bool keepsAlive_;
    bool countedKeepAlive_ = false;
    detail::TimerState state_ = detail::TimerState::Scheduled;
    doof::detail::CallbackDomain* owner_;
    doof::callback<void()> handler_;
};

inline void detail::MainEventDispatcher::addKeepAliveSource(bool keepsAlive) {
    doof::detail::ApplicationDomain::shared().add_keep_alive_source(keepsAlive);
}

inline void detail::MainEventDispatcher::removeKeepAliveSourceLocked(bool keepsAlive) {
    doof::detail::ApplicationDomain::shared().remove_keep_alive_source(keepsAlive);
}

inline bool detail::MainEventDispatcher::scheduleChannelLocked(
    const std::shared_ptr<NativeChannel>& channel
) {
    if (channel->scheduled_) {
        return false;
    }

    channel->scheduled_ = true;
    return true;
}

inline void detail::MainEventDispatcher::scheduleOwnerChannel(
    const std::shared_ptr<NativeChannel>& channel
) {
    auto owner = channel->owner_;
    if (!owner) {
        owner = &doof::detail::ApplicationDomain::shared();
    }

    owner->enqueue_callback([channel] {
        detail::MainEventDispatcher::shared().drainChannel(channel);
    });
}

inline int32_t detail::MainEventDispatcher::trySendMessage(
    const std::shared_ptr<NativeChannel>& channel,
    doof::callback<void()> task,
    bool hasKey,
    const std::string& key
) {
    bool shouldSchedule = false;
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

        if (scheduleChannelLocked(channel)) {
            shouldSchedule = true;
        }
    }

    if (shouldSchedule) {
        scheduleOwnerChannel(channel);
    }
    return code;
}

inline bool detail::MainEventDispatcher::tryClose(NativeChannel& channel) {
    bool removedKeepAlive = false;
    bool shouldSchedule = false;
    std::shared_ptr<NativeChannel> scheduledChannel;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel.closed_) {
            return false;
        }

        channel.closed_ = true;
        removedKeepAlive = detail::isApplicationDomain(channel.owner_) && channel.keepsAlive_;
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
                if (scheduleChannelLocked(self)) {
                    scheduledChannel = std::move(self);
                    shouldSchedule = true;
                }
            }
        }
    }

    if (shouldSchedule) {
        scheduleOwnerChannel(scheduledChannel);
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
            doof::detail::ApplicationDomain::shared().add_keep_alive_source(true);
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
            doof::detail::ApplicationDomain::shared().remove_keep_alive_source(true);
        }
    }

    return canceled;
}

inline void detail::MainEventDispatcher::commitTimer(const std::shared_ptr<NativeTimer>& timer) {
    doof::detail::CallbackDomain* owner = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timer->state_ != TimerState::Scheduled) {
            return;
        }

        timer->state_ = TimerState::Dispatching;
        owner = timer->owner_ ? timer->owner_ : &doof::detail::ApplicationDomain::shared();
    }

    owner->enqueue_callback([timer] {
        doof::detail::call_callback_unchecked(timer->handler_);
        detail::MainEventDispatcher::shared().finishTimerTick(*timer);
    });
}

inline void detail::MainEventDispatcher::finishTimerTick(NativeTimer& timer) {
    if (timer.kind_ == TimerKind::Timeout) {
        bool removedKeepAlive = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (timer.state_ == TimerState::Dispatching) {
                timer.state_ = TimerState::Completed;
            }
            if (timer.countedKeepAlive_) {
                timer.countedKeepAlive_ = false;
                removedKeepAlive = true;
            }
        }
        if (removedKeepAlive) {
            doof::detail::ApplicationDomain::shared().remove_keep_alive_source(true);
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

inline bool detail::MainEventDispatcher::takeChannelTaskLocked(
    const std::shared_ptr<NativeChannel>& channel,
    doof::callback<void()>& task
) {
    if (channel->tasks_.empty()) {
        return false;
    }

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

    return true;
}

inline void detail::MainEventDispatcher::drainChannel(
    const std::shared_ptr<NativeChannel>& channel
) {
    const int32_t batchSize = detail::isApplicationDomain(channel->owner_)
        ? 1
        : kActorChannelBatchSize;
    int32_t dispatched = 0;
    while (dispatched < batchSize) {
        doof::callback<void()> task;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!takeChannelTaskLocked(channel, task)) {
                channel->scheduled_ = false;
                return;
            }
        }

        doof::detail::call_callback_unchecked(task);
        ++dispatched;
    }

    bool shouldContinue = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel->tasks_.empty()) {
            channel->scheduled_ = false;
        } else {
            shouldContinue = true;
        }
    }

    if (shouldContinue) {
        scheduleOwnerChannel(channel);
    }
}

inline int32_t detail::MainEventDispatcher::drainReady() {
    return doof::detail::ApplicationDomain::shared().drain_ready();
}

inline bool detail::MainEventDispatcher::waitAndDispatchOne() {
    return doof::detail::ApplicationDomain::shared().wait_and_dispatch_one();
}

inline void detail::MainEventDispatcher::setWakeHandler(std::function<void()> handler) {
    doof::detail::ApplicationDomain::shared().set_wake_handler(std::move(handler));
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
        doof::detail::call_callback_unchecked(handler);
    });
}

inline void clearMainEventWakeHandler() {
    detail::MainEventDispatcher::shared().setWakeHandler(nullptr);
}

}  // namespace doof_event
