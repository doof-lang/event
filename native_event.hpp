#pragma once

#include "doof_runtime.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace doof_event {

class NativeTimer;

namespace detail {

inline doof::detail::CallbackDomain* currentOrApplicationDomain() {
    auto* owner = doof::current_actor_domain();
    if (owner) {
        return owner;
    }
    return &doof::detail::ApplicationDomain::shared();
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

// Timer state transitions share one lock so cancellation races cleanly with
// a tick that is about to be posted to the timer's owner.
class TimerScheduler {
public:
    static TimerScheduler& shared() {
        static TimerScheduler scheduler;
        return scheduler;
    }

    void start(const std::shared_ptr<NativeTimer>& timer);
    bool cancel(NativeTimer& timer);

private:
    TimerScheduler() = default;

    void commit(const std::shared_ptr<NativeTimer>& timer);
    void finishTick(NativeTimer& timer);

    std::mutex mutex_;
};

}  // namespace detail

// Keeps the root event loop running until released or dropped, for work
// whose next event the loop cannot see, such as a pending network reply.
class NativeMainEventLoopHold {
public:
    static std::shared_ptr<NativeMainEventLoopHold> retain() {
        doof::detail::ApplicationDomain::shared().add_keep_alive_source(true);
        return std::shared_ptr<NativeMainEventLoopHold>(new NativeMainEventLoopHold());
    }

    ~NativeMainEventLoopHold() {
        (void)release();
    }

    bool release() {
        if (released_.exchange(true)) {
            return false;
        }
        doof::detail::ApplicationDomain::shared().remove_keep_alive_source(true);
        return true;
    }

private:
    NativeMainEventLoopHold() = default;

    std::atomic<bool> released_{false};
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
        detail::TimerScheduler::shared().start(timer);
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
        detail::TimerScheduler::shared().start(timer);
        return timer;
    }

    bool cancel() {
        return detail::TimerScheduler::shared().cancel(*this);
    }

private:
    friend class detail::TimerScheduler;

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

inline void detail::TimerScheduler::start(const std::shared_ptr<NativeTimer>& timer) {
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
        detail::TimerScheduler::shared().commit(timer);
    }).detach();
}

inline bool detail::TimerScheduler::cancel(NativeTimer& timer) {
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

inline void detail::TimerScheduler::commit(const std::shared_ptr<NativeTimer>& timer) {
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
        detail::TimerScheduler::shared().finishTick(*timer);
    });
}

inline void detail::TimerScheduler::finishTick(NativeTimer& timer) {
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
        start(timer.shared_from_this());
    }
}

inline void runMainEventLoop() {
    while (doof::detail::ApplicationDomain::shared().wait_and_dispatch_one()) {
    }
}

inline int32_t drainMainEventLoop() {
    return doof::detail::ApplicationDomain::shared().drain_ready();
}

inline void setMainEventWakeHandler(std::function<void()> handler) {
    doof::detail::ApplicationDomain::shared().set_wake_handler(std::move(handler));
}

inline void setMainEventWakeCallback(doof::callback<void()> handler) {
    setMainEventWakeHandler([handler]() mutable {
        doof::detail::call_callback_unchecked(handler);
    });
}

inline void clearMainEventWakeHandler() {
    setMainEventWakeHandler(nullptr);
}

}  // namespace doof_event
