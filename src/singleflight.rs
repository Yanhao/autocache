use std::{collections::HashMap, future::Future, hash::Hash, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::{Error, Result};

pub(crate) struct Group<K, T>
where
    K: Eq + Hash,
    T: Clone,
{
    flights: Mutex<HashMap<K, Arc<Flight<T>>>>,
}

struct Flight<T>
where
    T: Clone,
{
    state: watch::Sender<State<T>>,
}

enum Role<T>
where
    T: Clone,
{
    Leader(Arc<Flight<T>>),
    Follower(watch::Receiver<State<T>>),
}

#[derive(Clone)]
enum State<T>
where
    T: Clone,
{
    Running,
    Done(Result<T>),
    LeaderDropped,
}

impl<K, T> Group<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    pub(crate) fn new() -> Self {
        Self {
            flights: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn work<F>(&self, key: K, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let mut future = Some(future);

        loop {
            let role = {
                let mut flights = self.flights.lock();

                if let Some(flight) = flights.get(&key) {
                    Role::Follower(flight.state.subscribe())
                } else {
                    let flight = Arc::new(Flight::new());
                    flights.insert(key.clone(), flight.clone());
                    Role::Leader(flight)
                }
            };

            match role {
                Role::Leader(flight) => {
                    let guard = LeaderGuard::new(self, key.clone(), flight.clone());
                    let Some(leader_future) = future.take() else {
                        return Err(Error::SingleFlight {
                            message: "leader future is unavailable".into(),
                        });
                    };
                    let result = leader_future.await;
                    let shared_result = result.clone();

                    flight.state.send_replace(State::Done(shared_result));
                    guard.finish();
                    return result;
                }
                Role::Follower(mut receiver) => loop {
                    let state = { receiver.borrow_and_update().clone() };
                    match state {
                        State::Running => {
                            if receiver.changed().await.is_err() {
                                break;
                            }
                        }
                        State::Done(result) => {
                            return result;
                        }
                        State::LeaderDropped => break,
                    }
                },
            }
        }
    }

    fn remove_if_current(&self, key: &K, flight: &Arc<Flight<T>>) {
        let mut flights = self.flights.lock();
        if flights
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, flight))
        {
            flights.remove(key);
        }
    }

    #[cfg(test)]
    fn follower_count(&self, key: &K) -> usize {
        self.flights
            .lock()
            .get(key)
            .map_or(0, |flight| flight.state.receiver_count())
    }
}

impl<T> Flight<T>
where
    T: Clone,
{
    fn new() -> Self {
        let (state, _) = watch::channel(State::Running);
        Self { state }
    }
}

struct LeaderGuard<'a, K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    group: &'a Group<K, T>,
    key: K,
    flight: Arc<Flight<T>>,
    finished: bool,
}

impl<'a, K, T> LeaderGuard<'a, K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn new(group: &'a Group<K, T>, key: K, flight: Arc<Flight<T>>) -> Self {
        Self {
            group,
            key,
            flight,
            finished: false,
        }
    }

    fn finish(mut self) {
        self.group.remove_if_current(&self.key, &self.flight);
        self.finished = true;
    }
}

impl<K, T> Drop for LeaderGuard<'_, K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        self.flight.state.send_replace(State::LeaderDropped);
        self.group.remove_if_current(&self.key, &self.flight);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::Group;
    use crate::{Error, LoaderKind};

    #[tokio::test]
    async fn concurrent_work_for_the_same_key_runs_once() {
        let group = Arc::new(Group::<String, usize>::new());
        let work_calls = Arc::new(AtomicUsize::new(0));
        let leader_started = Arc::new(tokio::sync::Notify::new());
        let release_leader = Arc::new(tokio::sync::Semaphore::new(0));
        let key = "test-key".to_string();

        let leader = {
            let group = group.clone();
            let work_calls = work_calls.clone();
            let leader_started = leader_started.clone();
            let release_leader = release_leader.clone();
            let key = key.clone();
            tokio::spawn(async move {
                group
                    .work(key, async move {
                        work_calls.fetch_add(1, Ordering::SeqCst);
                        leader_started.notify_one();
                        release_leader.acquire_owned().await.unwrap().forget();
                        Ok(42)
                    })
                    .await
            })
        };

        leader_started.notified().await;

        let follower = {
            let group = group.clone();
            let work_calls = work_calls.clone();
            let key = key.clone();
            tokio::spawn(async move {
                group
                    .work(key, async move {
                        work_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(99)
                    })
                    .await
            })
        };

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while group.follower_count(&key) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release_leader.add_permits(1);

        assert_eq!(leader.await.unwrap().unwrap(), 42);
        assert_eq!(follower.await.unwrap().unwrap(), 42);
        assert_eq!(work_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn follower_takes_over_after_leader_is_cancelled() {
        let group = Arc::new(Group::<String, usize>::new());
        let leader_started = Arc::new(tokio::sync::Notify::new());

        let leader = {
            let group = group.clone();
            let leader_started = leader_started.clone();
            tokio::spawn(async move {
                group
                    .work("test-key".to_string(), async move {
                        leader_started.notify_one();
                        std::future::pending::<crate::Result<usize>>().await
                    })
                    .await
            })
        };

        leader_started.notified().await;

        let follower = {
            let group = group.clone();
            tokio::spawn(async move {
                group
                    .work("test-key".to_string(), async move { Ok(42) })
                    .await
            })
        };

        leader.abort();

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), follower)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn followers_preserve_typed_errors() {
        let group = Arc::new(Group::<String, usize>::new());
        let leader_started = Arc::new(tokio::sync::Notify::new());
        let release_leader = Arc::new(tokio::sync::Semaphore::new(0));
        let key = "test-key".to_string();

        let leader = {
            let group = group.clone();
            let leader_started = leader_started.clone();
            let release_leader = release_leader.clone();
            let key = key.clone();
            tokio::spawn(async move {
                group
                    .work(key, async move {
                        leader_started.notify_one();
                        release_leader.acquire_owned().await.unwrap().forget();
                        Err(Error::loader(
                            LoaderKind::Single,
                            anyhow::anyhow!("loader failed"),
                        ))
                    })
                    .await
            })
        };

        leader_started.notified().await;

        let follower = {
            let group = group.clone();
            let key = key.clone();
            tokio::spawn(async move { group.work(key, async move { Ok(42) }).await })
        };

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while group.follower_count(&key) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release_leader.add_permits(1);

        for error in [
            leader.await.unwrap().unwrap_err(),
            follower.await.unwrap().unwrap_err(),
        ] {
            assert!(matches!(
                error,
                Error::Loader {
                    kind: LoaderKind::Single,
                    ..
                }
            ));
        }
    }
}
