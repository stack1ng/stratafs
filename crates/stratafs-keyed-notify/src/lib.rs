use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use event_listener::{Event, listener};

#[derive(Default)]
pub struct KeyedNotify {
    waiters: Mutex<HashMap<Bytes, Arc<Event>>>,
}

// KeyedNotify is a minimum allocation implementation of a keyed notify system.
impl KeyedNotify {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(keys: usize) -> Self {
        Self {
            waiters: Mutex::new(HashMap::with_capacity(keys)),
        }
    }

    pub async fn wait_until<E, F>(
        &self,
        key: &Bytes,
        mut condition: impl FnMut() -> F,
    ) -> Result<(), E>
    where
        F: Future<Output = Result<bool, E>>,
    {
        loop {
            let registration = self.register(key);
            listener!(registration.event => listener);
            if condition().await? {
                return Ok(());
            }
            if registration.current() {
                listener.await;
            }
        }
    }

    pub fn notify_waiters(&self, keys: &[Bytes]) {
        let mut waiters = self.lock();
        if waiters.is_empty() {
            return;
        }
        for key in keys {
            if let Some(event) = waiters.remove(key) {
                event.notify(usize::MAX);
            }
        }
    }

    fn register<'a>(&'a self, key: &'a Bytes) -> Registration<'a> {
        let event = self.lock().entry(key.clone()).or_default().clone();
        Registration {
            notify: self,
            key,
            event,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Bytes, Arc<Event>>> {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Registration<'a> {
    notify: &'a KeyedNotify,
    key: &'a Bytes,
    event: Arc<Event>,
}

impl Registration<'_> {
    fn current(&self) -> bool {
        self.notify
            .lock()
            .get(self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.event))
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut waiters = self.notify.lock();
        if let Some(current) = waiters.get(self.key)
            && Arc::ptr_eq(current, &self.event)
            && Arc::strong_count(&self.event) == 2
        {
            waiters.remove(self.key);
        }
    }
}
