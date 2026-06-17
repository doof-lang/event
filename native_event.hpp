#pragma once

#include "doof_runtime.hpp"

#include <any>
#include <cstdint>
#include <chrono>
#include <deque>
#include <functional>
#include <limits>
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
        std::function<void()> task,
        bool hasKey,
        const std::string& key
    );

    bool tryClose(NativeChannel& channel);

    void startTimer(const std::shared_ptr<NativeTimer>& timer);

    bool cancelTimer(NativeTimer& timer);

    void commitTimer(const std::shared_ptr<NativeTimer>& timer);

    void finishTimerTick(NativeTimer& timer);

    void drainChannel(const std::shared_ptr<NativeChannel>& channel);
    void drainSender(const std::shared_ptr<NativeChannel>& channel);
    void drainReceiver(const std::shared_ptr<NativeChannel>& channel);

    int32_t drainReady();

    bool waitAndDispatchOne();

    void setWakeHandler(std::function<void()> handler);

    void removeKeepAliveSourceLocked(bool keepsAlive);
    bool scheduleSenderLocked(const std::shared_ptr<NativeChannel>& channel);
    bool scheduleReceiverLocked(const std::shared_ptr<NativeChannel>& channel);
    void scheduleSenderChannel(const std::shared_ptr<NativeChannel>& channel);
    void scheduleReceiverChannel(const std::shared_ptr<NativeChannel>& channel);
    bool takeSenderTaskLocked(
        const std::shared_ptr<NativeChannel>& channel,
        std::function<void()>& task
    );
    bool takeReceiverTaskLocked(
        const std::shared_ptr<NativeChannel>& channel,
        std::function<void()>& task,
        bool& shouldScheduleSender
    );

    std::mutex mutex_;

private:
    MainEventDispatcher() = default;
};

}  // namespace detail

class NativeChannel : public std::enable_shared_from_this<NativeChannel> {
public:
    static std::shared_ptr<NativeChannel> createChannel(
        int32_t capacity,
        int32_t highWater,
        int32_t lowWater,
        bool keepsAlive
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
                keepsAlive
            )
        );
    }

    ~NativeChannel() {
        (void)tryClose();
    }

    template <typename T>
    int32_t trySendMessage(T value, bool hasKey, const std::string& key) {
        return detail::MainEventDispatcher::shared().trySendMessage(
            shared_from_this(),
            [self = shared_from_this(), value = std::move(value)]() mutable {
                self->deliverMessage<T>(std::move(value));
            },
            hasKey,
            key
        );
    }

    template <typename T>
    void registerReceiverMessage(doof::callback<void(T)> handler) {
        bool shouldSchedule = false;
        {
            std::lock_guard<std::mutex> lock(detail::MainEventDispatcher::shared().mutex_);
            claimReceiverOwnerLocked();
            if (receiverMessageRegistered_) {
                doof::panic("Channel receiver message handler is already registered");
            }
            receiverMessageHandler_ = std::function<void(T)>([handler = std::move(handler)](T value) mutable {
                handler.call(std::move(value));
            });
            receiverMessageRegistered_ = true;
            if (detail::MainEventDispatcher::shared().scheduleReceiverLocked(shared_from_this())) {
                shouldSchedule = true;
            }
        }
        if (shouldSchedule) {
            detail::MainEventDispatcher::shared().scheduleReceiverChannel(shared_from_this());
        }
    }

    template <typename T>
    void registerNativeReceiverMessage(std::function<void(T)> handler) {
        bool shouldSchedule = false;
        {
            std::lock_guard<std::mutex> lock(detail::MainEventDispatcher::shared().mutex_);
            setReceiverDispatchLocked(HandlerDispatch::Native);
            if (receiverMessageRegistered_) {
                doof::panic("Channel receiver message handler is already registered");
            }
            receiverMessageHandler_ = std::move(handler);
            receiverMessageRegistered_ = true;
            if (detail::MainEventDispatcher::shared().scheduleReceiverLocked(shared_from_this())) {
                shouldSchedule = true;
            }
        }
        if (shouldSchedule) {
            detail::MainEventDispatcher::shared().scheduleReceiverChannel(shared_from_this());
        }
    }

    void registerSenderReady(doof::callback<void()> handler) {
        registerSenderHandler(std::move(handler), SenderHandlerKind::Ready);
    }

    void registerSenderClosed(doof::callback<void()> handler) {
        registerSenderHandler(std::move(handler), SenderHandlerKind::Closed);
    }

    void registerReceiverClosed(doof::callback<void()> handler) {
        bool shouldSchedule = false;
        {
            std::lock_guard<std::mutex> lock(detail::MainEventDispatcher::shared().mutex_);
            claimReceiverOwnerLocked();
            if (receiverClosedRegistered_) {
                doof::panic("Channel receiver closed handler is already registered");
            }
            receiverClosedHandler_ = [handler = std::move(handler)]() mutable {
                handler.call();
            };
            receiverClosedRegistered_ = true;
            if (detail::MainEventDispatcher::shared().scheduleReceiverLocked(shared_from_this())) {
                shouldSchedule = true;
            }
        }
        if (shouldSchedule) {
            detail::MainEventDispatcher::shared().scheduleReceiverChannel(shared_from_this());
        }
    }

    void registerNativeSenderReady(std::function<void()> handler) {
        registerNativeSenderHandler(std::move(handler), SenderHandlerKind::Ready);
    }

    void registerNativeSenderClosed(std::function<void()> handler) {
        registerNativeSenderHandler(std::move(handler), SenderHandlerKind::Closed);
    }

    void registerNativeReceiverClosed(std::function<void()> handler) {
        bool shouldSchedule = false;
        {
            std::lock_guard<std::mutex> lock(detail::MainEventDispatcher::shared().mutex_);
            setReceiverDispatchLocked(HandlerDispatch::Native);
            if (receiverClosedRegistered_) {
                doof::panic("Channel receiver closed handler is already registered");
            }
            receiverClosedHandler_ = std::move(handler);
            receiverClosedRegistered_ = true;
            if (detail::MainEventDispatcher::shared().scheduleReceiverLocked(shared_from_this())) {
                shouldSchedule = true;
            }
        }
        if (shouldSchedule) {
            detail::MainEventDispatcher::shared().scheduleReceiverChannel(shared_from_this());
        }
    }

    void pauseReceiver() {
        std::lock_guard<std::mutex> lock(detail::MainEventDispatcher::shared().mutex_);
        receiverPaused_ = true;
    }

    void resumeReceiver() {
        bool shouldSchedule = false;
        {
            std::lock_guard<std::mutex> lock(detail::MainEventDispatcher::shared().mutex_);
            receiverPaused_ = false;
            if (detail::MainEventDispatcher::shared().scheduleReceiverLocked(shared_from_this())) {
                shouldSchedule = true;
            }
        }
        if (shouldSchedule) {
            detail::MainEventDispatcher::shared().scheduleReceiverChannel(shared_from_this());
        }
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
        bool keepsAlive
    ) : capacity_(capacity),
        highWater_(highWater),
        lowWater_(lowWater),
        keepsAlive_(keepsAlive) {
        detail::MainEventDispatcher::shared().addKeepAliveSource(keepsAlive_);
    }

    enum class TaskKind {
        Message,
        Ready,
        Closed,
    };

    struct ReceiverTask {
        TaskKind kind;
        bool hasKey;
        std::string key;
        std::function<void()> task;
    };

    struct SenderTask {
        TaskKind kind;
        std::function<void()> task;
    };

    enum class SenderHandlerKind {
        Ready,
        Closed,
    };

    enum class HandlerDispatch {
        Unset,
        Doof,
        Native,
    };

    void setSenderDispatchLocked(HandlerDispatch dispatch) {
        if (senderDispatch_ != HandlerDispatch::Unset && senderDispatch_ != dispatch) {
            doof::panic("Channel sender handlers must use the same dispatch mode");
        }
        senderDispatch_ = dispatch;
    }

    void setReceiverDispatchLocked(HandlerDispatch dispatch) {
        if (receiverDispatch_ != HandlerDispatch::Unset && receiverDispatch_ != dispatch) {
            doof::panic("Channel receiver handlers must use the same dispatch mode");
        }
        receiverDispatch_ = dispatch;
    }

    void claimSenderOwnerLocked() {
        auto* owner = detail::currentOrApplicationDomain();
        if (senderOwnerSet_ && senderOwner_ != owner) {
            doof::panic("Channel sender handlers must be registered from the same actor domain");
        }
        setSenderDispatchLocked(HandlerDispatch::Doof);
        senderOwner_ = owner;
        senderOwnerSet_ = true;
    }

    void claimReceiverOwnerLocked() {
        auto* owner = detail::currentOrApplicationDomain();
        if (receiverOwnerSet_ && receiverOwner_ != owner) {
            doof::panic("Channel receiver handlers must be registered from the same actor domain");
        }
        setReceiverDispatchLocked(HandlerDispatch::Doof);
        receiverOwner_ = owner;
        receiverOwnerSet_ = true;
    }

    void registerSenderHandler(doof::callback<void()> handler, SenderHandlerKind kind) {
        registerNativeSenderHandler(
            [handler = std::move(handler)]() mutable {
                handler.call();
            },
            kind,
            true
        );
    }

    void registerNativeSenderHandler(
        std::function<void()> handler,
        SenderHandlerKind kind,
        bool claimOwner = false
    ) {
        bool shouldSchedule = false;
        {
            std::lock_guard<std::mutex> lock(detail::MainEventDispatcher::shared().mutex_);
            if (claimOwner) {
                claimSenderOwnerLocked();
            } else {
                setSenderDispatchLocked(HandlerDispatch::Native);
            }
            if (kind == SenderHandlerKind::Ready) {
                if (senderReadyRegistered_) {
                    doof::panic("Channel sender ready handler is already registered");
                }
                senderReadyHandler_ = std::move(handler);
                senderReadyRegistered_ = true;
            } else {
                if (senderClosedRegistered_) {
                    doof::panic("Channel sender closed handler is already registered");
                }
                senderClosedHandler_ = std::move(handler);
                senderClosedRegistered_ = true;
            }
            if (detail::MainEventDispatcher::shared().scheduleSenderLocked(shared_from_this())) {
                shouldSchedule = true;
            }
        }
        if (shouldSchedule) {
            detail::MainEventDispatcher::shared().scheduleSenderChannel(shared_from_this());
        }
    }

    template <typename T>
    void deliverMessage(T value) {
        auto* handler = std::any_cast<std::function<void(T)>>(&receiverMessageHandler_);
        if (handler == nullptr) {
            doof::panic("Channel receiver message handler is not registered");
        }
        (*handler)(std::move(value));
    }

    int32_t capacity_;
    int32_t highWater_ = 0;
    int32_t lowWater_ = 0;
    doof::detail::CallbackDomain* senderOwner_ = nullptr;
    doof::detail::CallbackDomain* receiverOwner_ = nullptr;
    bool keepsAlive_;
    bool closed_ = false;
    bool senderScheduled_ = false;
    bool receiverScheduled_ = false;
    bool senderOwnerSet_ = false;
    bool receiverOwnerSet_ = false;
    HandlerDispatch senderDispatch_ = HandlerDispatch::Unset;
    HandlerDispatch receiverDispatch_ = HandlerDispatch::Unset;
    bool senderReadyRegistered_ = false;
    bool senderClosedRegistered_ = false;
    bool receiverMessageRegistered_ = false;
    bool receiverClosedRegistered_ = false;
    bool receiverPaused_ = false;
    bool waitingForReady_ = false;
    bool receiverClosedQueued_ = false;
    bool senderClosedQueued_ = false;
    int32_t messageCount_ = 0;
    std::any receiverMessageHandler_;
    std::function<void()> senderReadyHandler_;
    std::function<void()> senderClosedHandler_;
    std::function<void()> receiverClosedHandler_;
    std::deque<ReceiverTask> receiverTasks_;
    std::deque<SenderTask> senderTasks_;
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

inline bool detail::MainEventDispatcher::scheduleSenderLocked(
    const std::shared_ptr<NativeChannel>& channel
) {
    if (channel->senderScheduled_ || channel->senderTasks_.empty()) {
        return false;
    }

    for (auto& task : channel->senderTasks_) {
        if (task.kind == NativeChannel::TaskKind::Ready && channel->senderReadyRegistered_) {
            channel->senderScheduled_ = true;
            return true;
        }
        if (task.kind == NativeChannel::TaskKind::Closed && channel->senderClosedRegistered_) {
            channel->senderScheduled_ = true;
            return true;
        }
    }

    return false;
}

inline bool detail::MainEventDispatcher::scheduleReceiverLocked(
    const std::shared_ptr<NativeChannel>& channel
) {
    if (channel->receiverPaused_ || channel->receiverScheduled_ || channel->receiverTasks_.empty()) {
        return false;
    }

    auto& task = channel->receiverTasks_.front();
    if (task.kind == NativeChannel::TaskKind::Message && !channel->receiverMessageRegistered_) {
        return false;
    }
    if (task.kind == NativeChannel::TaskKind::Closed && !channel->receiverClosedRegistered_) {
        return false;
    }

    channel->receiverScheduled_ = true;
    return true;
}

inline void detail::MainEventDispatcher::scheduleSenderChannel(
    const std::shared_ptr<NativeChannel>& channel
) {
    if (channel->senderDispatch_ == NativeChannel::HandlerDispatch::Native) {
        drainSender(channel);
        return;
    }

    auto owner = channel->senderOwner_;
    if (!owner) {
        owner = &doof::detail::ApplicationDomain::shared();
    }

    owner->enqueue_callback([channel] {
        detail::MainEventDispatcher::shared().drainSender(channel);
    });
}

inline void detail::MainEventDispatcher::scheduleReceiverChannel(
    const std::shared_ptr<NativeChannel>& channel
) {
    if (channel->receiverDispatch_ == NativeChannel::HandlerDispatch::Native) {
        drainReceiver(channel);
        return;
    }

    auto owner = channel->receiverOwner_;
    if (!owner) {
        owner = &doof::detail::ApplicationDomain::shared();
    }

    owner->enqueue_callback([channel] {
        detail::MainEventDispatcher::shared().drainReceiver(channel);
    });
}

inline int32_t detail::MainEventDispatcher::trySendMessage(
    const std::shared_ptr<NativeChannel>& channel,
    std::function<void()> task,
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
            for (auto& queued : channel->receiverTasks_) {
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

        channel->receiverTasks_.push_back(NativeChannel::ReceiverTask {
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

        if (scheduleReceiverLocked(channel)) {
            shouldSchedule = true;
        }
    }

    if (shouldSchedule) {
        scheduleReceiverChannel(channel);
    }
    return code;
}

inline bool detail::MainEventDispatcher::tryClose(NativeChannel& channel) {
    bool removedKeepAlive = false;
    bool shouldScheduleReceiver = false;
    bool shouldScheduleSender = false;
    std::shared_ptr<NativeChannel> self;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel.closed_) {
            return false;
        }

        channel.closed_ = true;
        removedKeepAlive = channel.keepsAlive_;
        removeKeepAliveSourceLocked(removedKeepAlive);

        self = channel.weak_from_this().lock();
        if (self && !channel.receiverClosedQueued_) {
            channel.receiverClosedQueued_ = true;
            channel.receiverTasks_.push_back(NativeChannel::ReceiverTask {
                NativeChannel::TaskKind::Closed,
                false,
                std::string(),
                [self] {
                    self->receiverClosedHandler_();
                },
            });
            if (scheduleReceiverLocked(self)) {
                shouldScheduleReceiver = true;
            }
        }
        if (self && !channel.senderClosedQueued_) {
            channel.senderClosedQueued_ = true;
            channel.senderTasks_.push_back(NativeChannel::SenderTask {
                NativeChannel::TaskKind::Closed,
                [self] {
                    self->senderClosedHandler_();
                },
            });
            if (scheduleSenderLocked(self)) {
                shouldScheduleSender = true;
            }
        }
    }

    if (shouldScheduleReceiver) {
        scheduleReceiverChannel(self);
    }
    if (shouldScheduleSender) {
        scheduleSenderChannel(self);
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

inline bool detail::MainEventDispatcher::takeSenderTaskLocked(
    const std::shared_ptr<NativeChannel>& channel,
    std::function<void()>& task
) {
    if (channel->senderTasks_.empty()) {
        return false;
    }

    for (auto it = channel->senderTasks_.begin(); it != channel->senderTasks_.end(); ++it) {
        if (
            (it->kind == NativeChannel::TaskKind::Ready && channel->senderReadyRegistered_) ||
            (it->kind == NativeChannel::TaskKind::Closed && channel->senderClosedRegistered_)
        ) {
            auto queued = std::move(*it);
            channel->senderTasks_.erase(it);
            task = std::move(queued.task);
            return true;
        }
    }

    return false;
}

inline bool detail::MainEventDispatcher::takeReceiverTaskLocked(
    const std::shared_ptr<NativeChannel>& channel,
    std::function<void()>& task,
    bool& shouldScheduleSender
) {
    if (channel->receiverTasks_.empty()) {
        return false;
    }
    if (channel->receiverPaused_) {
        return false;
    }

    auto& next = channel->receiverTasks_.front();
    if (next.kind == NativeChannel::TaskKind::Message && !channel->receiverMessageRegistered_) {
        return false;
    }
    if (next.kind == NativeChannel::TaskKind::Closed && !channel->receiverClosedRegistered_) {
        return false;
    }

    auto queued = std::move(next);
    channel->receiverTasks_.pop_front();
    task = std::move(queued.task);

    if (queued.kind == NativeChannel::TaskKind::Message) {
        --channel->messageCount_;
        if (
            channel->waitingForReady_ &&
            channel->messageCount_ <= channel->lowWater_
        ) {
            channel->waitingForReady_ = false;
            channel->senderTasks_.push_back(NativeChannel::SenderTask {
                NativeChannel::TaskKind::Ready,
                [channel] {
                    channel->senderReadyHandler_();
                },
            });
            if (scheduleSenderLocked(channel)) {
                shouldScheduleSender = true;
            }
        }
    }

    return true;
}

inline void detail::MainEventDispatcher::drainChannel(
    const std::shared_ptr<NativeChannel>& channel
) {
    drainReceiver(channel);
}

inline void detail::MainEventDispatcher::drainSender(
    const std::shared_ptr<NativeChannel>& channel
) {
    const int32_t batchSize = channel->senderDispatch_ == NativeChannel::HandlerDispatch::Native
        ? std::numeric_limits<int32_t>::max()
        : detail::isApplicationDomain(channel->senderOwner_)
        ? 1
        : kActorChannelBatchSize;
    int32_t dispatched = 0;
    while (dispatched < batchSize) {
        std::function<void()> task;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!takeSenderTaskLocked(channel, task)) {
                channel->senderScheduled_ = false;
                return;
            }
        }

        task();
        ++dispatched;
    }

    bool shouldContinue = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel->senderTasks_.empty()) {
            channel->senderScheduled_ = false;
        } else {
            shouldContinue = true;
        }
    }

    if (shouldContinue) {
        scheduleSenderChannel(channel);
    }
}

inline void detail::MainEventDispatcher::drainReceiver(
    const std::shared_ptr<NativeChannel>& channel
) {
    const int32_t batchSize = channel->receiverDispatch_ == NativeChannel::HandlerDispatch::Native
        ? std::numeric_limits<int32_t>::max()
        : detail::isApplicationDomain(channel->receiverOwner_)
        ? 1
        : kActorChannelBatchSize;
    int32_t dispatched = 0;
    while (dispatched < batchSize) {
        std::function<void()> task;
        bool shouldScheduleSender = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!takeReceiverTaskLocked(channel, task, shouldScheduleSender)) {
                channel->receiverScheduled_ = false;
                return;
            }
        }

        if (shouldScheduleSender) {
            scheduleSenderChannel(channel);
        }
        task();
        ++dispatched;
    }

    bool shouldContinue = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (channel->receiverTasks_.empty()) {
            channel->receiverScheduled_ = false;
        } else {
            shouldContinue = true;
        }
    }

    if (shouldContinue) {
        scheduleReceiverChannel(channel);
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

template <typename T>
inline int32_t trySendChannelMessage(
    const std::shared_ptr<NativeChannel>& channel,
    T value,
    bool hasKey,
    const std::string& key
) {
    return channel->trySendMessage<T>(std::move(value), hasKey, key);
}

template <typename T>
inline void registerChannelReceiverMessage(
    const std::shared_ptr<NativeChannel>& channel,
    doof::callback<void(T)> handler
) {
    channel->registerReceiverMessage<T>(std::move(handler));
}

}  // namespace doof_event
