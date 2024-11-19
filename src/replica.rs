use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{error, trace};

use crate::command::ReqCommand;

const BROADCAST_CAPACITY: usize = 64;
const MPSC_CAPACITY: usize = 64;

#[derive(Debug)]
pub struct ReplicaManager {
    write_tx: broadcast::Sender<ReqCommand>,
    /// The reason for using `std::sync::Mutex` rather than `tokio` one is that
    /// we can garantee we will free the lock quickly and won't hold the lock
    /// accross the await point.
    replica_sending: std::sync::RwLock<HashMap<SocketAddr, usize>>,
    sent_rx: Mutex<mpsc::Receiver<()>>,
    sent_tx: mpsc::Sender<()>
}

impl Default for ReplicaManager {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(MPSC_CAPACITY);
        Self {
            write_tx: broadcast::Sender::new(BROADCAST_CAPACITY),
            replica_sending: std::sync::RwLock::new(HashMap::new()),
            sent_rx: Mutex::new(rx),
            sent_tx: tx,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SendError(#[from] broadcast::error::SendError<ReqCommand>),
}

type ReplicaResult<T> = Result<T, Error>;

/// I'm pretty sure the lock won't be poisoned, so I just unwrap it.
impl ReplicaManager {
    pub fn subscribe(&self, addr: &SocketAddr) -> ReplicaSubscriber {
        self.replica_sending
            .write()
            .expect("subscribe locking error")
            .insert(addr.clone(), 0);
        ReplicaSubscriber {
            addr: addr.clone(),
            write_rx: self.write_tx.subscribe(),
            sent_tx: self.sent_tx.clone(),
            manager: self,
        }
    }

    pub fn notify(&self, command: ReqCommand) -> ReplicaResult<()> {
        if self.write_tx.receiver_count() != 0 {
            self.incr_sending();
            self.write_tx.send(command)?;
        }
        Ok(())
    }

    fn incr_sending(&self) {
        self.replica_sending
            .write()
            .expect("incr sending locking error")
            .iter_mut()
            .for_each(|(_, i)| *i += 1);
    }

    fn desc_sending(&self, addr: &SocketAddr) -> usize {
        *self
            .replica_sending
            .write()
            .expect("desc sending locking error")
            .entry(addr.clone())
            .and_modify(|i| *i -= 1)
            .or_insert(0)
    }

    fn count_finished(&self) -> usize {
        self.replica_sending
            .read()
            .expect("count finished locking error")
            .iter()
            .filter(|(_, i)| **i == 0)
            .count()
    }

    pub async fn wait(&self, n: usize, timeout: usize) -> usize {
        let finished = self.count_finished();
        trace!("waiting for {n} and finished {finished}");
        if finished >= n {
            return finished;
        }
        match tokio::time::timeout(Duration::from_millis(timeout as u64), self.wait_n(n)).await {
            Ok(n) => n,
            Err(_) => self.count_finished(),
        }
    }

    async fn wait_n(&self, n: usize) -> usize {
        trace!("waiting for {} replicas", n);
        while self.sent_rx.lock().await.recv().await.is_some() {
            let finished = self.count_finished();
            trace!("{} replicas finished", finished);
            if finished >= n {
                return finished;
            }
        }
        unreachable!("I'm sure the sent_tx won't close")
    }
}

pub struct ReplicaSubscriber<'a> {
    write_rx: broadcast::Receiver<ReqCommand>,
    sent_tx: mpsc::Sender<()>,
    addr: SocketAddr,
    manager: &'a ReplicaManager,
}


impl ReplicaSubscriber<'_> {
    pub async fn recv(&mut self) -> Result<ReqCommand, broadcast::error::RecvError> {
        self.write_rx.recv().await
    }

    pub async fn sent(&mut self) -> usize {
        let n = self.manager.desc_sending(&self.addr);
        trace!("replica propagated, remain message: {}", n);
        self.sent_tx
            .send(())
            .await
            .expect("The recv half won't close! I'm sure");
        n
    }
}

impl Drop for ReplicaSubscriber<'_> {
    fn drop(&mut self) {
        match self.manager.replica_sending.write() {
            Ok(mut map) => {
                map.remove(&self.addr);
            }
            Err(e) => {
                error!("locking error while droping subscriber: {:?}", e);
            }
        }
    }
}
