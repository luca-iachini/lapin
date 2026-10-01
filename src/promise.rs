use crate::{Error, ErrorKind, Result, listener::Listener};
use std::{
    fmt,
    future::Future,
    panic::RefUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};
use tracing::{Level, level_enabled, trace};

#[must_use = "Promise should be used or you can miss errors"]
pub(crate) struct Promise<T> {
    shared: Arc<Shared<T>>,
    listener: Listener,
}

impl<T> fmt::Debug for Promise<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Promise")
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        trace!(
            promise = %self.shared.marker(),
            "Dropping promise.",
        );
    }
}

impl<T> Promise<T> {
    pub(crate) fn new(marker: &str) -> (Self, PromiseResolver<T>) {
        let promise = Self::build(marker, None);
        let resolver = promise.resolver();
        (promise, resolver)
    }

    pub(crate) fn new_with_data(marker: &str, data: Result<T>) -> Self {
        Self::build(marker, Some(data))
    }

    fn build(marker: &str, data: Option<Result<T>>) -> Self {
        let shared = Shared::new(data, marker);
        let listener = shared.listener.clone();
        Self { shared, listener }
    }

    pub(crate) fn try_wait(&self) -> Option<Result<T>> {
        self.shared.take()
    }

    /// Called once, so that every resolver of this promise is a clone of the one returned here
    /// and they all share the one [`Resolvers`] whose count decides which of them goes last.
    fn resolver(&self) -> PromiseResolver<T> {
        PromiseResolver(Arc::new(Resolvers(self.shared.clone())))
    }
}

impl<T> Future for Promise<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            self.listener.arm();
            if let Some(data) = self.shared.take() {
                self.listener.disarm();
                return Poll::Ready(data);
            }
            match self.listener.poll(cx) {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Completes a [`Promise`]. Several clones of a resolver usually coexist, one per way the
/// promise can be completed: the broker's reply, a failure to write the frame, the connection
/// or channel going away. The first one to complete it wins and the others become no-ops.
///
/// Dropping the last clone without completing the promise rejects it, so that a caller is
/// never left waiting on a promise nothing can answer anymore.
pub(crate) struct PromiseResolver<T>(Arc<Resolvers<T>>);

impl<T> fmt::Debug for PromiseResolver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PromiseResolver")
    }
}

impl<T> Clone for PromiseResolver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> PromiseResolver<T> {
    pub(crate) fn resolve(&self, data: T) {
        self.complete(Ok(data))
    }

    pub(crate) fn reject(&self, error: Error) {
        self.complete(Err(error))
    }

    pub(crate) fn complete(&self, res: Result<T>) {
        trace!(
            promise = %self.shared().marker(),
            "Resolving promise.",
        );
        self.shared().set(res);
    }

    fn shared(&self) -> &Shared<T> {
        &self.0.0
    }
}

/// Owned by every clone of a [`PromiseResolver`], so that the last clone to go is the one that
/// drops this and the reference count saying so is [`Arc`]'s rather than one of our own.
struct Resolvers<T>(Arc<Shared<T>>);

impl<T> Drop for Resolvers<T> {
    /// Nothing is left to complete the promise once every resolver is gone, so it is rejected
    /// rather than left pending for good. Usually a no-op: every completion path but the winning
    /// one lets go of its resolver once the promise already has its answer.
    fn drop(&mut self) {
        self.0.set(Err(ErrorKind::PromiseAbandoned.into()));
    }
}

pub(crate) trait Cancelable {
    fn cancel(&self, err: Error);
}

impl<T> Cancelable for PromiseResolver<T> {
    fn cancel(&self, err: Error) {
        self.reject(err)
    }
}

/// Whether a promise has its answer yet, kept apart from whether it still holds it: a promise
/// that has handed its answer over is answered, so a resolver going away afterwards must not
/// put another one in its place.
enum Data<T> {
    Pending,
    Ready(Result<T>),
    Taken,
}

struct Shared<T> {
    data: Mutex<Data<T>>,
    listener: Listener,
    marker: Option<String>,
}

// Listener wraps event-listener::Event which uses UnsafeCell internally, opting
// out of RefUnwindSafe by default. The only panic vector is Waker::wake() inside
// notify(); if it panics the waiting task is not woken, but the data is already
// written before notify() is called so a subsequent poll will find it.
impl<T> RefUnwindSafe for Shared<T> where Result<T>: RefUnwindSafe {}

impl<T> Shared<T> {
    fn new(data: Option<Result<T>>, marker: &str) -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(data.map_or(Data::Pending, Data::Ready)),
            listener: Listener::default(),
            marker: if level_enabled!(Level::TRACE) {
                Some(marker.into())
            } else {
                None
            },
        })
    }

    fn set(&self, data: Result<T>) {
        let mut lock = self.lock_data();
        if matches!(*lock, Data::Pending) {
            *lock = Data::Ready(data);
            // Release the lock before waking to avoid the woken task
            // immediately blocking on it.
            drop(lock);
            self.listener.notify();
        }
    }

    fn take(&self) -> Option<Result<T>> {
        let mut lock = self.lock_data();
        match std::mem::replace(&mut *lock, Data::Taken) {
            Data::Ready(data) => Some(data),
            pending_or_taken => {
                *lock = pending_or_taken;
                None
            }
        }
    }

    fn lock_data(&self) -> MutexGuard<'_, Data<T>> {
        self.data.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn marker(&self) -> String {
        self.marker
            .as_ref()
            .map_or(String::default(), |marker| format!("[{marker}] "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Dropping the last resolver rejects the promise instead of leaving it pending for good.
    #[test]
    fn a_dropped_resolver_rejects_its_promise() {
        let (promise, resolver) = Promise::<()>::new("test.dropped-resolver");
        drop(resolver);
        assert!(matches!(
            promise.try_wait().unwrap().unwrap_err().kind(),
            ErrorKind::PromiseAbandoned
        ));
    }

    /// A promise that has handed its answer over keeps it: the resolvers going away afterwards
    /// must not put an abandonment error in its place.
    #[test]
    fn a_promise_that_gave_its_answer_away_is_not_answered_again() {
        let (promise, resolver) = Promise::<()>::new("test.answered-promise");
        resolver.resolve(());
        assert!(
            promise
                .try_wait()
                .expect("the promise was resolved")
                .is_ok()
        );
        drop(resolver);
        assert!(promise.try_wait().is_none());
    }

    /// A resolver that still has clones alive does not reject on drop, otherwise the first
    /// completion path to give up would beat the one that actually completes the promise.
    #[test]
    fn a_dropped_resolver_clone_leaves_its_promise_alone() {
        let (promise, resolver) = Promise::<()>::new("test.dropped-resolver-clone");
        drop(resolver.clone());
        assert!(
            promise.try_wait().is_none(),
            "the promise should still be waiting on the resolver that is left"
        );
    }
}
