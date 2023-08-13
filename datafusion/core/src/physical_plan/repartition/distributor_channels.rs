// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Special channel construction to distribute data from various inputs into N outputs
//! minimizing buffering but preventing deadlocks when repartitoning
//!
//! # Design
//!
//! ```text
//! +----+
//! | TX |==||
//! +----+  ||    +------------------+  +----+
//!         ======| Channel (buffer) |==| RX |
//! +----+  ||    +------------------+  +----+
//! | TX |==||
//! +----+
//!
//! +----+        +------------------+  +----+
//! | TX |========| Channel (buffer) |==| RX |
//! +----+        +------------------+  +----+
//! ```
//!
//! There are `N` virtual MPSC (multi-producer, single consumer) channels each
//! which their own bounded buffers. If a channel reaches its upper limit, data
//! will need to be pulled on the consuming side before additional data is sent.
//!
//! Cross channel communication is (currently) avoided.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

/// Max size of each channel's buffer.
const MAX_CHANNEL_BUFFER_SIZE: usize = 8;
const SEND_BUFFER_SIZE: usize = 2;

/// Create `n` empty channels.
pub fn channels<T>(
    n: usize,
) -> (Vec<DistributionSender<T>>, Vec<DistributionReceiver<T>>) {
    let channels = (0..n)
        .map(|_| {
            Arc::new(SharedChannel {
                channel: Mutex::new(Channel {
                    data: VecDeque::new(),
                    n_senders: 1,
                    recv_alive: true,
                    recv_waker: None,
                    send_wakers: Vec::new(),
                }),
            })
        })
        .collect::<Vec<_>>();
    let senders = channels
        .iter()
        .map(|channel| DistributionSender {
            buf: Vec::with_capacity(SEND_BUFFER_SIZE),
            channel: Arc::clone(channel),
        })
        .collect();
    let receivers = channels
        .into_iter()
        .map(|channel| DistributionReceiver {
            channel,
            recv_buf: VecDeque::new(),
        })
        .collect();
    (senders, receivers)
}

type PartitionAwareSenders<T> = Vec<Vec<DistributionSender<T>>>;
type PartitionAwareReceivers<T> = Vec<Vec<DistributionReceiver<T>>>;

/// Create `n_out` empty channels for each of the `n_in` inputs.
/// This way, each distinct partition will communicate via a dedicated channel.
/// This SPSC structure enables us to track which partition input data comes from.
pub fn partition_aware_channels<T>(
    n_in: usize,
    n_out: usize,
) -> (PartitionAwareSenders<T>, PartitionAwareReceivers<T>) {
    (0..n_in).map(|_| channels(n_out)).unzip()
}

/// Erroring during [send](DistributionSender::send).
///
/// This occurs when the [receiver](DistributionReceiver) is gone.
#[derive(PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SendError").finish()
    }
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot send data, receiver is gone")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Sender side of distribution [channels].
///
/// This handle can be cloned. All clones will write into the same channel. Dropping the last sender will close the
/// channel. In this case, the [receiver](DistributionReceiver) will still be able to poll the remaining data, but will
/// receive `None` afterwards.
#[derive(Debug)]
pub struct DistributionSender<T> {
    buf: Vec<T>,
    channel: Arc<SharedChannel<T>>,
}

impl<T> DistributionSender<T> {
    /// Send data.
    ///
    /// This fails if the [receiver](DistributionReceiver) is gone.
    pub fn send(&mut self, element: T) -> SendFuture<'_, T> {
        SendFuture {
            buf: &mut self.buf,
            channel: &self.channel,
            element: Some(element),
        }
    }
}

impl<T> Clone for DistributionSender<T> {
    fn clone(&self) -> Self {
        let mut guard = self.channel.channel.lock();
        guard.n_senders += 1;

        Self {
            buf: Vec::with_capacity(SEND_BUFFER_SIZE),
            channel: Arc::clone(&self.channel),
        }
    }
}

impl<T> Drop for DistributionSender<T> {
    fn drop(&mut self) {
        let mut guard_channel = self.channel.channel.lock();
        guard_channel.data.extend(self.buf.drain(..));
        guard_channel.n_senders -= 1;

        if guard_channel.n_senders == 0 {
            // receiver may be waiting for data, but should return `None` now since the channel is closed
            guard_channel.wake_receiver();
        }
    }
}

/// Future backing [send](DistributionSender::send).
#[derive(Debug)]
pub struct SendFuture<'a, T> {
    buf: &'a mut Vec<T>,
    channel: &'a SharedChannel<T>,
    element: Option<T>,
}

impl<'a, T: Unpin> Future for SendFuture<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(self.element.is_some(), "polled ready future");

        let mut guard_channel = match self.channel.channel.try_lock() {
            Some(g) => g,
            None if self.buf.len() < SEND_BUFFER_SIZE => {
                let element = self.element.take().unwrap();
                self.buf.push(element);
                return Poll::Ready(Ok(()));
            }
            None => self.channel.channel.lock(),
        };

        // receiver end still alive?
        if !guard_channel.recv_alive {
            return Poll::Ready(Err(SendError(
                self.element.take().expect("just checked"),
            )));
        }

        if guard_channel.data.len() >= MAX_CHANNEL_BUFFER_SIZE {
            // We've reached the buffer limit. Store the waker until we have
            // more room in the buffer.
            guard_channel.send_wakers.push(cx.waker().clone());
            // Try to make more room by waking the receiver.
            let waker = guard_channel.recv_waker.take();
            std::mem::drop(guard_channel);
            if let Some(waker) = waker {
                waker.wake();
            }
            Poll::Pending
        } else {
            guard_channel.data.extend(self.buf.drain(..));
            guard_channel
                .data
                .push_back(self.element.take().expect("just checked"));

            let mut waker = guard_channel.recv_waker.take();
            if waker.is_none() {
                waker = guard_channel.send_wakers.pop();
            }

            std::mem::drop(guard_channel);
            if let Some(waker) = waker {
                waker.wake();
            }
            Poll::Ready(Ok(()))
        }
    }
}

/// Receiver side of distribution [channels].
#[derive(Debug)]
pub struct DistributionReceiver<T> {
    recv_buf: VecDeque<T>,
    channel: Arc<SharedChannel<T>>,
}

impl<T> DistributionReceiver<T> {
    /// Receive data from channel.
    ///
    /// Returns `None` if the channel is empty and no [senders](DistributionSender) are left.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture {
            recv_buf: &mut self.recv_buf,
            channel: &self.channel,
            rdy: false,
        }
    }
}

impl<T> Drop for DistributionReceiver<T> {
    fn drop(&mut self) {
        let mut guard_channel = self.channel.channel.lock();
        guard_channel.recv_alive = false;

        // senders may be waiting for gate to open but should error now that the channel is closed
        guard_channel.wake_senders();

        // clear potential remaining data from channel
        guard_channel.data.clear();
    }
}

/// Future backing [recv](DistributionReceiver::recv).
pub struct RecvFuture<'a, T> {
    recv_buf: &'a mut VecDeque<T>,
    channel: &'a SharedChannel<T>,
    rdy: bool,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.rdy, "polled ready future");

        if let Some(b) = self.recv_buf.pop_front() {
            return Poll::Ready(Some(b));
        }

        let mut guard_channel = self.channel.channel.lock();

        match guard_channel.data.pop_front() {
            Some(element) => {
                self.recv_buf.extend(guard_channel.data.drain(..));
                // Only wake a single sender since we've only make room for one
                // piece of data.
                guard_channel.wake_a_sender();
                self.rdy = true;
                Poll::Ready(Some(element))
            }
            None if guard_channel.n_senders == 0 => {
                self.rdy = true;
                Poll::Ready(None)
            }
            None => {
                guard_channel.recv_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Links senders and receivers.
#[derive(Debug)]
struct Channel<T> {
    /// Buffered data.
    data: VecDeque<T>,

    /// Reference counter for the sender side.
    n_senders: usize,

    /// Reference "counter"/flag for the single receiver.
    recv_alive: bool,

    /// Waker for the receiver side.
    ///
    /// The receiver will be pending if the [buffer](Self::data) is empty and
    /// there are senders left (according to the [reference counter](Self::n_senders)).
    recv_waker: Option<Waker>,

    /// Wakers for sender side.
    ///
    /// Send wakers will be stored when the channel buffer is full.
    send_wakers: Vec<Waker>,
}

impl<T> Channel<T> {
    /// Wake the receiver.
    fn wake_receiver(&mut self) {
        if let Some(waker) = self.recv_waker.take() {
            waker.wake();
        }
    }

    /// Wake all senders.
    fn wake_senders(&mut self) {
        for waker in self.send_wakers.drain(..) {
            waker.wake();
        }
    }

    /// Wake a single sender. Which sender to wake is picked arbitrarily.
    fn wake_a_sender(&mut self) {
        if let Some(waker) = self.send_wakers.pop() {
            waker.wake();
        }
    }
}

/// Shared channel.
///
/// One or multiple senders and a single receiver will share a channel.
#[derive(Debug)]
struct SharedChannel<T> {
    channel: Mutex<Channel<T>>,
}

// #[cfg(test)]
// mod tests {
//     use std::sync::atomic::{AtomicBool, Ordering};

//     use futures::{task::ArcWake, FutureExt};

//     use super::*;

//     #[test]
//     fn test_single_channel() {
//         let (mut txs, mut rxs) = channels(1);

//         let mut recv_fut = rxs[0].recv();
//         let waker = poll_pending(&mut recv_fut);

//         poll_ready(&mut txs[0].send("foo")).unwrap();
//         assert!(waker.woken());
//         assert_eq!(poll_ready(&mut recv_fut), Some("foo"),);

//         poll_ready(&mut txs[0].send("bar")).unwrap();
//         poll_ready(&mut txs[0].send("baz")).unwrap();
//         poll_ready(&mut txs[0].send("end")).unwrap();
//         assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"),);
//         assert_eq!(poll_ready(&mut rxs[0].recv()), Some("baz"),);

//         // close channel
//         txs.remove(0);
//         assert_eq!(poll_ready(&mut rxs[0].recv()), Some("end"),);
//         assert_eq!(poll_ready(&mut rxs[0].recv()), None,);
//         assert_eq!(poll_ready(&mut rxs[0].recv()), None,);
//     }

//     #[test]
//     fn test_multi_sender() {
//         let (txs, mut rxs) = channels(1);

//         let tx_clone = txs[0].clone();

//         poll_ready(&mut txs[0].send("foo")).unwrap();
//         poll_ready(&mut tx_clone.send("bar")).unwrap();

//         assert_eq!(poll_ready(&mut rxs[0].recv()), Some("foo"),);
//         assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"),);
//     }

//     #[test]
//     fn test_hit_buffer_limit() {
//         let (txs, mut rxs) = channels(1);

//         // Fill buffer.
//         for i in 0..MAX_CHANNEL_BUFFER_SIZE {
//             poll_ready(&mut txs[0].send(i as u64)).unwrap();
//         }

//         // Try to push another.
//         let waker = poll_pending(&mut txs[0].send(999));

//         // Make more room.
//         assert_eq!(poll_ready(&mut rxs[0].recv()), Some(0),);

//         // Check that our waker was woken now that there's room.
//         assert!(waker.woken());
//     }

//     #[test]
//     fn test_close_channel_by_dropping_tx() {
//         let (mut txs, mut rxs) = channels(2);

//         let tx0 = txs.remove(0);
//         let tx1 = txs.remove(0);
//         let tx0_clone = tx0.clone();

//         let mut recv_fut = rxs[0].recv();

//         poll_ready(&mut tx1.send("a")).unwrap();
//         let recv_waker = poll_pending(&mut recv_fut);

//         // drop original sender
//         drop(tx0);

//         // not yet closed (there's a clone left)
//         assert!(!recv_waker.woken());
//         poll_ready(&mut tx1.send("b")).unwrap();
//         let recv_waker = poll_pending(&mut recv_fut);

//         // create new clone
//         let tx0_clone2 = tx0_clone.clone();
//         assert!(!recv_waker.woken());
//         poll_ready(&mut tx1.send("c")).unwrap();
//         let recv_waker = poll_pending(&mut recv_fut);

//         // drop first clone
//         drop(tx0_clone);
//         assert!(!recv_waker.woken());
//         poll_ready(&mut tx1.send("d")).unwrap();
//         let recv_waker = poll_pending(&mut recv_fut);

//         // drop last clone
//         drop(tx0_clone2);

//         assert!(recv_waker.woken());
//         assert_eq!(poll_ready(&mut recv_fut), None);
//     }

//     // #[test]
//     // fn test_drop_rx_three_channels() {
//     //     let (mut txs, mut rxs) = channels(3);

//     //     let tx0 = txs.remove(0);
//     //     let tx1 = txs.remove(0);
//     //     let tx2 = txs.remove(0);
//     //     let mut rx0 = rxs.remove(0);
//     //     let rx1 = rxs.remove(0);
//     //     let _rx2 = rxs.remove(0);

//     //     // fill channels
//     //     poll_ready(&mut tx0.send("0_a")).unwrap();
//     //     poll_ready(&mut tx1.send("1_a")).unwrap();
//     //     poll_ready(&mut tx2.send("2_a")).unwrap();

//     //     // drop / close one channel
//     //     drop(rx1);

//     //     // receive data
//     //     assert_eq!(poll_ready(&mut rx0.recv()), Some("0_a"),);

//     //     // use senders again
//     //     poll_ready(&mut tx0.send("0_b")).unwrap();
//     //     assert_eq!(poll_ready(&mut tx1.send("1_b")), Err(SendError("1_b")),);
//     //     poll_ready(&mut tx2.send("2_b")).unwrap();
//     // }

//     #[test]
//     fn test_close_channel_by_dropping_rx_clears_data() {
//         let (txs, rxs) = channels(1);

//         let obj = Arc::new(());
//         let counter = Arc::downgrade(&obj);
//         assert_eq!(counter.strong_count(), 1);

//         // add object to channel
//         poll_ready(&mut txs[0].send(obj)).unwrap();
//         assert_eq!(counter.strong_count(), 1);

//         // drop receiver
//         drop(rxs);

//         assert_eq!(counter.strong_count(), 0);
//     }

//     #[test]
//     #[should_panic(expected = "polled ready future")]
//     fn test_panic_poll_send_future_after_ready_ok() {
//         let (txs, _rxs) = channels(1);
//         let mut fut = txs[0].send("foo");
//         poll_ready(&mut fut).unwrap();
//         poll_ready(&mut fut).ok();
//     }

//     #[test]
//     #[should_panic(expected = "polled ready future")]
//     fn test_panic_poll_send_future_after_ready_err() {
//         let (txs, rxs) = channels(1);

//         drop(rxs);

//         let mut fut = txs[0].send("foo");
//         poll_ready(&mut fut).unwrap_err();
//         poll_ready(&mut fut).ok();
//     }

//     #[test]
//     #[should_panic(expected = "polled ready future")]
//     fn test_panic_poll_recv_future_after_ready_some() {
//         let (txs, mut rxs) = channels(1);

//         poll_ready(&mut txs[0].send("foo")).unwrap();

//         let mut fut = rxs[0].recv();
//         poll_ready(&mut fut).unwrap();
//         poll_ready(&mut fut);
//     }

//     #[test]
//     #[should_panic(expected = "polled ready future")]
//     fn test_panic_poll_recv_future_after_ready_none() {
//         let (txs, mut rxs) = channels::<u8>(1);

//         drop(txs);

//         let mut fut = rxs[0].recv();
//         assert!(poll_ready(&mut fut).is_none());
//         poll_ready(&mut fut);
//     }

//     #[test]
//     #[should_panic(expected = "future is pending")]
//     fn test_meta_poll_ready_wrong_state() {
//         let mut fut = futures::future::pending::<u8>();
//         poll_ready(&mut fut);
//     }

//     #[test]
//     #[should_panic(expected = "future is ready")]
//     fn test_meta_poll_pending_wrong_state() {
//         let mut fut = futures::future::ready(1);
//         poll_pending(&mut fut);
//     }

//     #[test]
//     fn test_meta_poll_pending_waker() {
//         let (tx, mut rx) = futures::channel::oneshot::channel();
//         let waker = poll_pending(&mut rx);
//         assert!(!waker.woken());
//         tx.send(1).unwrap();
//         assert!(waker.woken());
//     }

//     /// Poll a given [`Future`] and ensure it is [ready](Poll::Ready).
//     #[track_caller]
//     fn poll_ready<F>(fut: &mut F) -> F::Output
//     where
//         F: Future + Unpin,
//     {
//         match poll(fut).0 {
//             Poll::Ready(x) => x,
//             Poll::Pending => panic!("future is pending"),
//         }
//     }

//     /// Poll a given [`Future`] and ensure it is [pending](Poll::Pending).
//     ///
//     /// Returns a waker that can later be checked.
//     #[track_caller]
//     fn poll_pending<F>(fut: &mut F) -> Arc<TestWaker>
//     where
//         F: Future + Unpin,
//     {
//         let (res, waker) = poll(fut);
//         match res {
//             Poll::Ready(_) => panic!("future is ready"),
//             Poll::Pending => waker,
//         }
//     }

//     fn poll<F>(fut: &mut F) -> (Poll<F::Output>, Arc<TestWaker>)
//     where
//         F: Future + Unpin,
//     {
//         let test_waker = Arc::new(TestWaker::default());
//         let waker = futures::task::waker(Arc::clone(&test_waker));
//         let mut cx = std::task::Context::from_waker(&waker);
//         let res = fut.poll_unpin(&mut cx);
//         (res, test_waker)
//     }

//     /// A test [`Waker`] that signal if [`wake`](Waker::wake) was called.
//     #[derive(Debug, Default)]
//     struct TestWaker {
//         woken: AtomicBool,
//     }

//     impl TestWaker {
//         /// Was [`wake`](Waker::wake) called?
//         fn woken(&self) -> bool {
//             self.woken.load(Ordering::SeqCst)
//         }
//     }

//     impl ArcWake for TestWaker {
//         fn wake_by_ref(arc_self: &Arc<Self>) {
//             arc_self.woken.store(true, Ordering::SeqCst);
//         }
//     }
// }
