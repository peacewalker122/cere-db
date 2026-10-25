use crate::{api::api::BatchOperation, error::DBError};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

pub(crate) const MAX_ADMITTED_REQUESTS: u32 = 64;
pub(crate) const ADMITTED_BYTE_BUDGET: u32 = 16 * 1024 * 1024;
pub(crate) const MAX_DRAIN_REQUESTS: usize = 64;
pub(crate) const MAX_DRAIN_BYTES: usize = ADMITTED_BYTE_BUDGET as usize;

#[derive(Clone)]
pub(crate) struct AdmissionQueue(Arc<QueueInner>);

struct QueueInner {
    state: Mutex<State>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    completed: Notify,
}

struct State {
    requests: VecDeque<Request>,
    running: bool,
    next_ticket: u64,
    completed_ticket: u64,
}

pub(crate) struct Request {
    pub(crate) operations: Vec<BatchOperation>,
    pub(crate) ticket: u64,
    completion: oneshot::Sender<Result<(), DBError>>,
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    byte_weight: usize,
}

pub(crate) struct Enqueued {
    pub(crate) ticket: u64,
    pub(crate) completion: oneshot::Receiver<Result<(), DBError>>,
    pub(crate) should_start_runner: bool,
}

impl AdmissionQueue {
    pub(crate) fn new() -> Self {
        Self(Arc::new(QueueInner {
            state: Mutex::new(State {
                requests: VecDeque::new(),
                running: false,
                next_ticket: 0,
                completed_ticket: 0,
            }),
            slots: Arc::new(Semaphore::new(MAX_ADMITTED_REQUESTS as usize)),
            bytes: Arc::new(Semaphore::new(ADMITTED_BYTE_BUDGET as usize)),
            completed: Notify::new(),
        }))
    }

    pub(crate) async fn enqueue(
        &self,
        operations: Vec<BatchOperation>,
        estimated_bytes: usize,
    ) -> Result<Enqueued, DBError> {
        let slot = self
            .0
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DBError::StorageError("write admission is closed".into()))?;
        let byte_permits = estimated_bytes.min(ADMITTED_BYTE_BUDGET as usize).max(1) as u32;
        let bytes = self
            .0
            .bytes
            .clone()
            .acquire_many_owned(byte_permits)
            .await
            .map_err(|_| DBError::StorageError("write admission is closed".into()))?;
        let (completion, receiver) = oneshot::channel();
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| DBError::MutexPoisoned("write admission queue".into()))?;
        let ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| DBError::StorageError("write admission ticket exhausted".into()))?;
        state.next_ticket = ticket;
        state.requests.push_back(Request {
            operations,
            ticket,
            completion,
            _slot: slot,
            _bytes: bytes,
            byte_weight: estimated_bytes,
        });
        let should_start_runner = !state.running;
        state.running = true;
        Ok(Enqueued {
            ticket,
            completion: receiver,
            should_start_runner,
        })
    }

    pub(crate) fn drain_ready(
        &self,
        max_requests: usize,
        max_bytes: usize,
    ) -> Result<Option<Vec<Request>>, DBError> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| DBError::MutexPoisoned("write admission queue".into()))?;
        if state.requests.is_empty() {
            state.running = false;
            return Ok(None);
        }
        let mut requests = Vec::new();
        let mut bytes = 0usize;
        while requests.len() < max_requests {
            let Some(next) = state.requests.front() else {
                break;
            };
            let exceeds_byte_limit = !requests.is_empty()
                && (next.byte_weight > max_bytes.saturating_sub(bytes)
                    || requests
                        .iter()
                        .any(|request: &Request| request.byte_weight > max_bytes));
            if exceeds_byte_limit {
                break;
            }
            let request = state.requests.pop_front().expect("front was present");
            bytes = bytes.saturating_add(request.byte_weight);
            let oversized = request.byte_weight > max_bytes;
            requests.push(request);
            if oversized {
                break;
            }
        }
        Ok(Some(requests))
    }

    pub(crate) fn complete(
        &self,
        request: Request,
        result: Result<(), DBError>,
    ) -> Result<(), DBError> {
        let Request {
            ticket, completion, ..
        } = request;
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| DBError::MutexPoisoned("write admission queue".into()))?;
        if ticket != state.completed_ticket + 1 || ticket > state.next_ticket {
            return Err(DBError::StorageError(
                "out-of-order write completion".into(),
            ));
        }
        state.completed_ticket = ticket;
        drop(state);
        let _ = completion.send(result);
        self.0.completed.notify_waiters();
        Ok(())
    }

    pub(crate) fn last_admitted_ticket(&self) -> Result<u64, DBError> {
        self.0
            .state
            .lock()
            .map(|state| state.next_ticket)
            .map_err(|_| DBError::MutexPoisoned("write admission queue".into()))
    }

    pub(crate) async fn await_completed_through(&self, ticket: u64) -> Result<(), DBError> {
        loop {
            let notified = self.0.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let complete = self
                .0
                .state
                .lock()
                .map_err(|_| DBError::MutexPoisoned("write admission queue".into()))?
                .completed_ticket
                >= ticket;
            if complete {
                return Ok(());
            }
            notified.await;
        }
    }
}

impl Default for AdmissionQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    fn operation(key: u8) -> Vec<BatchOperation> {
        vec![BatchOperation::Put {
            key: vec![key],
            value: vec![key],
        }]
    }

    async fn enqueue(queue: &AdmissionQueue, key: u8, bytes: usize) -> Enqueued {
        queue.enqueue(operation(key), bytes).await.unwrap()
    }

    #[tokio::test]
    async fn drains_fifo_in_bounded_batches() {
        let queue = AdmissionQueue::new();
        let first = enqueue(&queue, 1, 4).await;
        let second = enqueue(&queue, 2, 4).await;
        let third = enqueue(&queue, 3, 4).await;
        assert!(first.should_start_runner);
        assert!(!second.should_start_runner);
        let batch = queue.drain_ready(2, 8).unwrap().unwrap();
        assert_eq!(
            batch
                .iter()
                .map(|request| request.ticket)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(queue.drain_ready(2, 8).unwrap().unwrap()[0].ticket, 3);
        assert_eq!(third.ticket, 3);
    }

    #[tokio::test]
    async fn oversized_request_acquires_the_entire_byte_budget() {
        let queue = AdmissionQueue::new();
        let oversized = enqueue(&queue, 1, ADMITTED_BYTE_BUDGET as usize + 9).await;
        let waiter_queue = queue.clone();
        let waiter = tokio::spawn(async move { enqueue(&waiter_queue, 2, 1).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        queue
            .complete(
                queue.drain_ready(1, usize::MAX).unwrap().unwrap().remove(0),
                Ok(()),
            )
            .unwrap();
        assert_eq!(waiter.await.unwrap().ticket, 2);
        drop(oversized);
    }

    #[tokio::test]
    async fn admission_backpressures_and_cancellation_never_enqueues() {
        let queue = AdmissionQueue::new();
        let mut held = Vec::new();
        for i in 0..MAX_ADMITTED_REQUESTS {
            held.push(enqueue(&queue, i as u8, 1).await);
        }
        let queued = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_queue = queue.clone();
        let task_queued = queued.clone();
        let task = tokio::spawn(async move {
            let result = task_queue.enqueue(operation(250), 1).await;
            task_queued.store(result.is_ok(), std::sync::atomic::Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        assert!(!queued.load(std::sync::atomic::Ordering::SeqCst));
        task.abort();
        let _ = task.await;
        assert_eq!(
            queue.last_admitted_ticket().unwrap(),
            u64::from(MAX_ADMITTED_REQUESTS)
        );
        drop(held);
        assert!(
            queue
                .drain_ready(MAX_ADMITTED_REQUESTS as usize, usize::MAX)
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn barrier_waits_for_snapshot_and_ignores_later_admissions() {
        let queue = AdmissionQueue::new();
        let first = enqueue(&queue, 1, 1).await;
        let snapshot = queue.last_admitted_ticket().unwrap();
        let requests = queue.drain_ready(1, 1).unwrap().unwrap();
        let barrier_queue = queue.clone();
        let barrier = tokio::spawn(async move {
            barrier_queue
                .await_completed_through(snapshot)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        assert!(!barrier.is_finished());
        let second = enqueue(&queue, 2, 1).await;
        let mut requests = requests.into_iter();
        queue.complete(requests.next().unwrap(), Ok(())).unwrap();
        barrier.await.unwrap();
        assert!(
            queue
                .complete(queue.drain_ready(1, 1).unwrap().unwrap().remove(0), Ok(()))
                .is_ok()
        );
        assert!(first.completion.await.unwrap().is_ok());
        assert!(second.completion.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn drained_requests_hold_admission_permits_until_completion() {
        let queue = AdmissionQueue::new();
        let mut admitted = Vec::new();
        for key in 0..MAX_ADMITTED_REQUESTS {
            admitted.push(enqueue(&queue, key as u8, 1).await);
        }
        let mut requests = queue.drain_ready(1, 1).unwrap().unwrap();
        let waiter_queue = queue.clone();
        let waiter = tokio::spawn(async move { enqueue(&waiter_queue, 2, 1).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        queue.complete(requests.pop().unwrap(), Ok(())).unwrap();
        let next = waiter.await.unwrap();
        assert_eq!(next.ticket, u64::from(MAX_ADMITTED_REQUESTS) + 1);
        assert!(admitted.remove(0).completion.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn empty_transition_elects_exactly_one_new_runner() {
        let queue = AdmissionQueue::new();
        let first = enqueue(&queue, 1, 1).await;
        queue.drain_ready(1, 1).unwrap().unwrap();
        assert!(queue.drain_ready(1, 1).unwrap().is_none());
        let next = enqueue(&queue, 2, 1).await;
        let follower = enqueue(&queue, 3, 1).await;
        assert!(next.should_start_runner);
        assert!(!follower.should_start_runner);
        assert_eq!(first.ticket, 1);
    }

    #[tokio::test]
    async fn byte_budget_and_oversized_requests_are_fifo_and_solo() {
        let queue = AdmissionQueue::new();
        let oversized = enqueue(&queue, 1, ADMITTED_BYTE_BUDGET as usize + 1).await;
        let first_batch = queue
            .drain_ready(4, ADMITTED_BYTE_BUDGET as usize)
            .unwrap()
            .unwrap();
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].ticket, oversized.ticket);
        queue
            .complete(first_batch.into_iter().next().unwrap(), Ok(()))
            .unwrap();
        let following = enqueue(&queue, 2, 1).await;
        let following_batch = queue
            .drain_ready(4, ADMITTED_BYTE_BUDGET as usize)
            .unwrap()
            .unwrap();
        assert_eq!(following_batch[0].ticket, following.ticket);
    }

    #[tokio::test]
    async fn complete_rejects_out_of_order_tickets_without_panicking() {
        let queue = AdmissionQueue::new();
        enqueue(&queue, 1, 1).await;
        let request = queue.drain_ready(1, 1).unwrap().unwrap().remove(0);
        assert!(queue.complete(request, Ok(())).is_ok());
        assert!(matches!(
            queue.complete(
                Request {
                    operations: Vec::new(),
                    ticket: 1,
                    completion: oneshot::channel().0,
                    _slot: queue.0.slots.clone().try_acquire_owned().unwrap(),
                    _bytes: queue.0.bytes.clone().try_acquire_many_owned(1).unwrap(),
                    byte_weight: 0,
                },
                Ok(())
            ),
            Err(DBError::StorageError(_))
        ));
    }
}
