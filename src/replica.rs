use std::{io, time::Duration};
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{error, trace, warn};

use crate::message::Message;

const BROADCAST_CAPACITY: usize = 64;
const MPSC_CAPACITY: usize = 64;

#[derive(Debug)]
pub struct ReplicaManager {
    replica_notify_tx: broadcast::Sender<ReplicaNotifyMessage>,
    replica_need_acked: Mutex<usize>,
    sent_rx: Mutex<mpsc::Receiver<usize>>,
    sent_tx: mpsc::Sender<usize>,
}

impl Default for ReplicaManager {
    fn default() -> Self {
        // TODO: may recreate this channel when waiting replicas, and send the tx to the replica thread.
        let (tx, rx) = mpsc::channel(MPSC_CAPACITY);
        Self {
            replica_notify_tx: broadcast::Sender::new(BROADCAST_CAPACITY),
            replica_need_acked: Mutex::new(0),
            sent_rx: Mutex::new(rx),
            sent_tx: tx,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SendError(#[from] broadcast::error::SendError<ReplicaNotifyMessage>),
    #[error(transparent)]
    IoError(#[from] io::Error),
}

type ReplicaResult<T> = Result<T, Error>;

/// I'm pretty sure the lock won't be poisoned, so I just unwrap it.
impl ReplicaManager {
    pub fn subscribe(&self) -> broadcast::Receiver<ReplicaNotifyMessage> {
        self.replica_notify_tx.subscribe()
    }

    pub async fn notify(&self, message: impl Into<Message>) -> ReplicaResult<()> {
        let message = message.into();
        let bytes_len = message.count_bytes()?;
        self.incr_sending(bytes_len).await;
        if self.replica_notify_tx.receiver_count() != 0 {
            self.replica_notify_tx
                .send(ReplicaNotifyMessage::Propagated(message.into()))?;
        }
        Ok(())
    }

    pub fn replicas(&self) -> usize {
        self.replica_notify_tx.receiver_count()
    }

    async fn incr_sending(&self, len: usize) {
        *self.replica_need_acked.lock().await += len;
    }

    pub async fn ack(&self, len: usize) {
        self.sent_tx.send(len).await.unwrap();
    }

    pub async fn wait(&self, n: usize, timeout: usize) -> ReplicaResult<usize> {
        let mut finished = 0;
        let need_acked = { *self.replica_need_acked.lock().await };
        if need_acked == 0 {
            // nothing to sync, every replica is finished
            return Ok(n.min(self.replicas()));
        }
        self.replica_notify_tx.send(ReplicaNotifyMessage::Wait)?;
        match tokio::time::timeout(
            Duration::from_millis(timeout as u64),
            self.wait_n(n, &mut finished, need_acked),
        )
        .await
        {
            Ok(n) => Ok(n),
            Err(_) => {
                warn!("waiting time out, only {finished} replica finished");
                Ok(finished)
            }
        }
    }

    async fn wait_n(&self, n: usize, finished: &mut usize, need_acked: usize) -> usize {
        trace!("waiting for {} replicas", n);
        while let Some(len) = self.sent_rx.lock().await.recv().await {
            if len >= need_acked {
                *finished += 1;
                if *finished >= n {
                    return *finished;
                }
            }
        }
        unreachable!("I'm sure the sent_tx won't close")
    }
}

#[derive(Clone, Debug)]
pub enum ReplicaNotifyMessage {
    Propagated(Message),
    Wait,
}

pub struct ReplicaSubscriber {
    replica_notify_rx: broadcast::Receiver<ReplicaNotifyMessage>,
}

impl ReplicaSubscriber {
    pub async fn recv(&mut self) -> Result<ReplicaNotifyMessage, broadcast::error::RecvError> {
        self.replica_notify_rx.recv().await
    }
}
