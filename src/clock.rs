//! Provider-neutral monotonic clock and deterministic scheduler seams.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock},
    time::Duration,
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{error::ModelError, model::MonoInstant};

pub type ClockFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait Clock: Send + Sync {
    fn now(&self) -> MonoInstant;
    fn sleep_until<'a>(&'a self, deadline: MonoInstant) -> ClockFuture<'a>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> MonoInstant {
        system_now()
    }

    fn sleep_until<'a>(&'a self, deadline: MonoInstant) -> ClockFuture<'a> {
        Box::pin(sleep_until(deadline))
    }
}

#[derive(Clone, Debug)]
pub struct ManualClock {
    state: Arc<watch::Sender<MonoInstant>>,
}

impl ManualClock {
    #[must_use]
    pub fn new(initial: MonoInstant) -> Self {
        let (sender, _) = watch::channel(initial);
        Self {
            state: Arc::new(sender),
        }
    }

    pub fn advance_to(&self, deadline: MonoInstant) -> Result<(), ModelError> {
        if deadline < self.now() {
            return Err(ModelError::InvalidMonoInstant);
        }
        self.state.send_replace(deadline);
        Ok(())
    }

    pub fn advance_by(&self, duration: Duration) -> Result<MonoInstant, ModelError> {
        let deadline = checked_deadline(self.now(), duration)?;
        self.state.send_replace(deadline);
        Ok(deadline)
    }
}

impl Clock for ManualClock {
    fn now(&self) -> MonoInstant {
        *self.state.borrow()
    }

    fn sleep_until<'a>(&'a self, deadline: MonoInstant) -> ClockFuture<'a> {
        let mut receiver = self.state.subscribe();
        Box::pin(async move {
            while *receiver.borrow_and_update() < deadline {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        })
    }
}

pub fn checked_deadline(now: MonoInstant, duration: Duration) -> Result<MonoInstant, ModelError> {
    let additional =
        u64::try_from(duration.as_nanos()).map_err(|_| ModelError::InvalidMonoInstant)?;
    now.as_nanos()
        .checked_add(additional)
        .map(MonoInstant::from_nanos)
        .ok_or(ModelError::InvalidMonoInstant)
}

pub async fn sleep_until(deadline: MonoInstant) {
    let now = system_now();
    if deadline <= now {
        return;
    }
    tokio::time::sleep(Duration::from_nanos(deadline.as_nanos() - now.as_nanos())).await;
}

pub async fn sleep_until_or_cancelled<C: Clock + ?Sized>(
    clock: &C,
    deadline: MonoInstant,
    cancellation: &CancellationToken,
) -> SleepOutcome {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => SleepOutcome::Cancelled,
        () = clock.sleep_until(deadline) => SleepOutcome::Deadline,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SleepOutcome {
    Deadline,
    Cancelled,
}

fn system_now() -> MonoInstant {
    static EPOCH: LazyLock<std::time::Instant> = LazyLock::new(std::time::Instant::now);
    let elapsed = EPOCH.elapsed();
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    MonoInstant::from_nanos(nanos)
}
