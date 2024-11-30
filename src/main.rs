use anyhow::{bail, Context};
use futures_util::{SinkExt, StreamExt};
use hex_literal::hex;
use redis_starter_rust::{
    command::{
        ConfigSubCommand, ReplConfSubresponse, ReplconfSubcommand, ReqCommand, RespCommand,
        XReadItem,
    },
    db::{Db, EntryId, StreamEntry, Value, ValueError},
    message::{Message, MessageFramer, RespError},
    rdb,
    replica::{ReplicaManager, ReplicaNotifyMessage},
};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    sync::{broadcast, RwLock},
};
use tokio_util::codec;
use tracing::{debug, error, info, trace, trace_span, warn, Instrument, Level};

use clap::Parser;

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

const EMPTY_RDB_CONTENT: &[u8] = &hex!("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    dir: Option<String>,
    #[arg(long)]
    dbfilename: Option<String>,
    #[arg(long, default_value = "6379")]
    port: u16,
    #[arg(long, value_parser = parse_socket_addr_v4)]
    replicaof: Option<SocketAddrV4>,
}

fn parse_socket_addr_v4(s: &str) -> Result<SocketAddrV4, String> {
    let (ip, port) = s.split_once(" ").ok_or("invalid socket address")?;
    let ip = if ip == "localhost" {
        Ipv4Addr::LOCALHOST
    } else {
        ip.parse().map_err(|_| "invalid ip address")?
    };
    let port = port.parse().map_err(|_| "invalid port")?;
    Ok(SocketAddrV4::new(ip, port))
}

impl Args {
    fn is_master(&self) -> bool {
        self.replicaof.is_none()
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    let log_level = if std::env::var("REDIS_LOG").is_ok() {
        Level::TRACE
    } else {
        Level::WARN
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();
    let args = Args::parse();
    let db =
        rdb::init(&args.dir, &args.dbfilename).context("init from redis database file error")?;

    let self_addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), args.port);
    let listener = TcpListener::bind(&self_addr).await.unwrap();
    let db = Arc::new(RwLock::new(db));
    let config = Arc::new(args);

    let stream_manager = Arc::new(StreamManager::default());
    let replica_manager = Arc::new(ReplicaManager::default());

    if let Some(replicaof) = config.replicaof.clone() {
        let db = db.clone();
        let config = config.clone();
        let stream_manager = stream_manager.clone();
        let replica_manager = replica_manager.clone();
        tokio::spawn(async move {
            let socket = handshake_with_master(replicaof, config.clone())
                .await
                .expect("handshake with master");
            process_client_socket::<false>(socket, db, config, stream_manager, replica_manager)
                .await
                .expect("process socket erorr");
        });
    }

    loop {
        let (socket, _) = listener.accept().await?;
        let addr = socket.peer_addr()?;
        let db = db.clone();
        let config = config.clone();
        let stream_manager = stream_manager.clone();
        let state = replica_manager.clone();
        tokio::spawn(
            async move {
                let socket = tokio_util::codec::Framed::new(socket, MessageFramer::default());
                process_client_socket::<true>(socket, db, config, stream_manager, state)
                    .await
                    .inspect_err(|e| error!("process socket error: {:?}", e))
                    .expect("process socket error")
            }
            .instrument(trace_span!("client", addr = %addr)),
        );
    }
}

async fn handshake_with_master(
    replicaof: SocketAddrV4,
    config: Arc<Args>,
) -> anyhow::Result<codec::Framed<TcpStream, MessageFramer>> {
    let master = TcpStream::connect(replicaof)
        .await
        .context("cannot connect to master")?;
    let mut master = tokio_util::codec::Framed::new(master, MessageFramer::default());
    master.send(ReqCommand::Ping.into()).await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")?
        .context("decode response message error")?
        .0
        .parse_resp()?;
    trace!("response of ping: {:?}", response);
    debug_assert!(matches!(response, RespCommand::Pong));

    master
        .send(ReqCommand::Replconf(ReplconfSubcommand::ListeningPort(config.port)).into())
        .await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of replconf: {:?}", response);
    debug_assert!(matches!(response, RespCommand::Ok));

    master
        .send(ReqCommand::Replconf(ReplconfSubcommand::Capa("psync2".to_string())).into())
        .await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of replconf: {:?}", response);
    debug_assert!(matches!(response, RespCommand::Ok));

    master
        .send(
            ReqCommand::Psync {
                id: None,
                offset: None,
            }
            .into(),
        )
        .await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of psync: {:?}", response);
    debug_assert!(matches!(response, RespCommand::FullResync { .. }));

    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of handshake: {:?}", response);
    assert!(matches!(response, RespCommand::RdbFile(_)));
    Ok(master)
}

#[derive(Default)]
struct RedisContext {
    received_bytes: usize,
    transaction: Option<Vec<(ReqCommand, usize)>>,
}

async fn process_client_socket<const NOT_SLAVE: bool>(
    mut socket: codec::Framed<TcpStream, MessageFramer>,
    db: Arc<RwLock<Db>>,
    cfg: Arc<Args>,
    stream_manager: Arc<StreamManager>,
    replica_manager: Arc<ReplicaManager>,
) -> anyhow::Result<()> {
    let mut ctx = RedisContext::default();
    while let Some(message) = socket.next().await {
        trace!("received message: {:?}", message);
        let (message, message_bytes) = message?;
        let req_command = message.parse_req()?;
        info!("parsed command: {:?}", req_command);

        match req_command {
            ReqCommand::Exec => {
                let Some(transaction) = ctx.transaction.take() else {
                    socket.send(RespError::ExecWithoutMulti.into()).await?;
                    continue;
                };
                let mut responds = Vec::with_capacity(transaction.len());
                for (cmd, bytes) in transaction {
                    let message = respond_to::<NOT_SLAVE>(
                        &cmd,
                        &mut socket,
                        &db,
                        &cfg,
                        &stream_manager,
                        &replica_manager,
                        &mut ctx,
                    )
                    .await?
                    .unwrap_or(RespCommand::Nil.into());
                    ctx.received_bytes += bytes;
                    responds.push(message);
                }
                socket.send(Message::Arrays(responds)).await?;
                ctx.received_bytes += message_bytes;
                continue;
            }
            ReqCommand::Discard => {
                if ctx.transaction.take().is_none() {
                    socket.send(RespError::DiscardWithoutMulti.into()).await?;
                } else {
                    socket.send(RespCommand::Ok.into()).await?;
                }
                continue;
            }
            _ => {}
        }

        if let Some(transaction) = ctx.transaction.as_mut() {
            transaction.push((req_command.clone(), message_bytes));
            socket.send(RespCommand::Simple("QUEUED").into()).await?;
            continue;
        }
        let message = respond_to::<NOT_SLAVE>(
            &req_command,
            &mut socket,
            &db,
            &cfg,
            &stream_manager,
            &replica_manager,
            &mut ctx,
        )
        .await?;

        ctx.received_bytes += message_bytes;

        let Some(message) = message else {
            continue;
        };
        if NOT_SLAVE {
            socket.send(message).await?;
        }
    }
    Ok(())
}

async fn respond_to<const NOT_SLAVE: bool>(
    req_command: &ReqCommand,
    socket: &mut codec::Framed<TcpStream, MessageFramer>,
    db: &RwLock<Db>,
    cfg: &Args,
    stream_manager: &StreamManager,
    replica_manager: &ReplicaManager,
    ctx: &mut RedisContext,
) -> anyhow::Result<Option<Message>> {
    let message: Message = match &req_command {
        ReqCommand::Ping => RespCommand::Pong.into(),
        ReqCommand::Echo(data) => RespCommand::Bulk(data.clone()).into(),
        ReqCommand::Get(key) => {
            let (value, expire) = {
                let db = db.read().await;
                (db.kv.get(key).cloned(), db.expire.get(key).cloned())
            };
            let value = if let Some(value) = value {
                if let Some(expire) = expire {
                    if UNIX_EPOCH + Duration::from_millis(expire) < std::time::SystemTime::now() {
                        RespCommand::Nil
                    } else {
                        value.into()
                    }
                } else {
                    value.into()
                }
            } else {
                RespCommand::Nil
            };
            value.into()
        }
        ReqCommand::Set { key, value, px } => {
            replica_manager
                .notify(req_command.clone())
                .await
                .context("notify message to replica error")?;
            let expired_time = px.and_then(|px| {
                (std::time::SystemTime::now() + px)
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|d| d.as_millis() as u64)
            });
            {
                let mut db = db.write().await;
                db.kv.insert(key.clone(), Value::String(value.clone()));

                if let Some(expired_time) = expired_time {
                    db.expire.insert(key.clone(), expired_time);
                } else {
                    db.expire.remove_entry(key);
                }
            }
            RespCommand::Simple("OK").into()
        }
        ReqCommand::Config(ConfigSubCommand::GET(key)) => {
            let value = match key.as_str() {
                "dir" => cfg.dir.clone(),
                "dbfilename" => cfg.dbfilename.clone(),
                _ => None,
            };
            RespCommand::Bulks(vec![Some(key.clone()), value]).into()
        }
        ReqCommand::KEYS(pattern) => {
            let keys = {
                let db = db.read().await;
                db.kv.keys().cloned().collect::<Vec<String>>()
            };
            let keys = keys
                .into_iter()
                .filter(|key| simple_pattern_match(&pattern, key))
                .map(Option::Some)
                .collect();
            RespCommand::Bulks(keys).into()
        }
        ReqCommand::Info(section) => match section.as_ref().map(|s| s.as_str()) {
            Some("replication") => {
                let info = replication_info(cfg)
                    .into_iter()
                    .map(|(k, v)| format!("{}:{}", k, v))
                    .collect::<Vec<String>>()
                    .join("\r\n");
                RespCommand::Bulk(info).into()
            }
            None => {
                let info = all_info(&cfg)
                    .into_iter()
                    .map(|(k, v)| format!("{}:{}", k, v))
                    .collect::<Vec<String>>()
                    .join("\r\n");
                RespCommand::Bulk(info).into()
            }
            s => {
                error!("unsupported INFO section: {:?}", s);
                return Ok(None);
            }
        },
        ReqCommand::Replconf(subc) => {
            if NOT_SLAVE {
                RespCommand::Ok.into()
            } else {
                match subc {
                    ReplconfSubcommand::Getack => {
                        socket
                            .send(
                                RespCommand::Replconf(ReplConfSubresponse::Ack(ctx.received_bytes))
                                    .into(),
                            )
                            .await?;
                    }
                    _ => warn!("only response getack when in replica mode"),
                }
                return Ok(None);
            }
        }
        ReqCommand::Psync { .. } => {
            socket
                .send(
                    RespCommand::FullResync {
                        repl_id: REPLICATION_ID.to_string(),
                        offset: 0,
                    }
                    .into(),
                )
                .await?;
            socket
                .send(RespCommand::RdbFile(EMPTY_RDB_CONTENT.to_vec()).into())
                .await?;

            return process_replica_socket(socket, replica_manager)
                .await
                .map(|_| None);
        }
        ReqCommand::Wait {
            number_replicas,
            time_out,
        } => {
            // replica number is assert to small than i64::max;
            let replicas = replica_manager.wait(*number_replicas, *time_out).await?;
            RespCommand::Int(replicas as i64).into()
        }
        ReqCommand::Type(key) => {
            let ty = {
                let db = db.read().await;
                Value::tyn(db.kv.get(key))
            };
            RespCommand::Simple(ty).into()
        }
        ReqCommand::XADD {
            stream_key,
            entry_id,
            pairs,
        } => {
            let push_res = {
                let mut db = db.write().await;
                let db_entry = db
                    .kv
                    .entry(stream_key.clone())
                    .or_insert_with(|| Value::Stream(vec![]));
                db_entry.push_stream_entry(&entry_id, pairs.clone())
            };
            match push_res {
                Ok(id) => {
                    if stream_manager.stream_notify_tx.receiver_count() != 0 {
                        stream_manager.stream_notify_tx.send(())?;
                    }
                    RespCommand::Bulk(id.to_string()).into()
                }
                Err(ValueError::RespError(resp)) => {
                    warn!("{resp}");
                    Message::SimpleErrors(resp.to_string())
                }
                Err(e) => return Err(e.into()),
            }
        }
        ReqCommand::XRANGE {
            stream_key,
            start_id,
            end_id,
        } => {
            let entries = {
                let db = db.read().await;
                let db_entry = db.kv.get(stream_key);
                db_entry
                    .map(|entry| entry.xrange(start_id, end_id))
                    .transpose()?
                    .unwrap_or_default()
            };

            RespCommand::StreamEntries(entries).into()
        }
        ReqCommand::XREAD {
            streams: items,
            block_time,
        } => {
            async fn read_entries<'items>(
                db: &RwLock<Db>,
                items: &'items Vec<redis_starter_rust::command::XReadItem>,
            ) -> anyhow::Result<Vec<(&'items String, Vec<StreamEntry>)>> {
                let db = db.read().await;
                Ok(items
                    .iter()
                    .map(|item| {
                        let db_entry = db.kv.get(&item.stream_key);
                        Ok((
                            &item.stream_key,
                            db_entry
                                .map(|entry: &Value| entry.xread(&item.start))
                                .transpose()?
                                .unwrap_or_default(),
                        ))
                    })
                    .collect::<Result<Vec<_>, ValueError>>()?
                    .into_iter()
                    .filter(|(_, items)| !items.is_empty())
                    .collect())
            }

            let items = {
                let db = db.read().await;
                items
                    .iter()
                    .map(|item| {
                        let id = db
                            .kv
                            .get(&item.stream_key)
                            .map(|entry| entry.map_xread_raw(&item.start))
                            .transpose()?
                            .unwrap_or(EntryId::ZERO);
                        Ok(XReadItem {
                            stream_key: item.stream_key.clone(),
                            start: id,
                        })
                    })
                    .collect::<Result<_, ValueError>>()?
            };
            let streams = read_entries(db, &items).await?;
            let streams = if streams.is_empty() {
                if let Some(block_time) = block_time {
                    debug!("blocking: {block_time}");
                    let mut stream_rx = stream_manager.stream_notify_tx.subscribe();

                    let async_lookp = async {
                        loop {
                            stream_rx.recv().await?;
                            let streams = read_entries(db, &items).await?;
                            if !streams.is_empty() {
                                break anyhow::Result::<Vec<(&String, Vec<StreamEntry>)>>::Ok(
                                    streams,
                                );
                            }
                        }
                    };
                    if *block_time == 0 {
                        async_lookp.await?
                    } else {
                        let block_result = tokio::time::timeout(
                            Duration::from_millis(*block_time as u64),
                            async_lookp,
                        )
                        .await;
                        match block_result {
                            Ok(streams) => streams?,
                            Err(_) => {
                                debug!("timeout");
                                vec![]
                            } // timeout
                        }
                    }
                } else {
                    streams
                }
            } else {
                streams
            };
            let response = streams
                .into_iter()
                .map(|(key, entries)| {
                    let stream_key = Message::BulkStrings(Some(key.clone()));
                    let entries = Message::from(RespCommand::StreamEntries(entries));
                    Message::Arrays(vec![stream_key, entries])
                })
                .collect::<Vec<_>>();
            if response.is_empty() {
                RespCommand::Nil.into()
            } else {
                Message::Arrays(response)
            }
        }
        ReqCommand::Incr(key) => {
            let value = {
                let mut db = db.write().await;
                let value = db.kv.entry(key.clone()).or_insert(Value::Integer(0));
                value.incr()
            };
            match value {
                Ok(value) => RespCommand::Int(value).into(),
                Err(ValueError::RespError(resp)) => Message::SimpleErrors(resp.to_string()),
                Err(e) => return Err(e.into()),
            }
        }
        ReqCommand::Multi => {
            // replica may not need transaction
            if NOT_SLAVE {
                ctx.transaction.replace(vec![]);
                RespCommand::Ok.into()
            } else {
                return Ok(None);
            }
        }
        cmd => {
            error!("unsupported command: {:?}", cmd);
            return Ok(None);
        }
    };
    Ok(Some(message))
}

async fn process_replica_socket(
    socket: &mut codec::Framed<TcpStream, MessageFramer>,
    manager: &ReplicaManager,
) -> anyhow::Result<()> {
    info!("replica connected.");
    let (mut socket_tx, mut socket_rx) = socket.split();
    let mut replica_broadcast_rx = manager.subscribe();

    loop {
        select! {
            message = replica_broadcast_rx.recv() => {
                let propagated = message?;
                trace!("replica received broadcast message: {:?}", propagated);
                match propagated {
                    ReplicaNotifyMessage::Propagated(propagated) => {
                        socket_tx.send(propagated).await?;
                    }
                    ReplicaNotifyMessage::Wait => {
                        socket_tx
                        .send(ReqCommand::Replconf(ReplconfSubcommand::Getack).into())
                        .await?;
                    }
                }
            }
            message = socket_rx.next() => {
                trace!("received message: {:?}", message);
                let (message, _) = message.context("connection closed by remote")??;
                let resp_command = message.parse_resp()?;
                info!("parsed command: {resp_command:?}");
                match &resp_command {
                    RespCommand::Replconf(ReplConfSubresponse::Ack(n)) => {
                        manager.ack(*n).await;
                    }
                    resp => {
                        bail!("unaccpet response in replica: {resp:?}")
                    }
                }
            }
        }
    }
}

fn replication_info(cfg: &Args) -> HashMap<String, String> {
    let mut info = HashMap::new();
    if cfg.is_master() {
        info.insert("role".to_string(), "master".to_string());
        info.insert("master_replid".to_string(), REPLICATION_ID.to_string());
        info.insert("master_repl_offset".to_string(), "0".to_string());
    } else {
        info.insert("role".to_string(), "slave".to_string());
    }
    info
}

fn all_info(cfg: &Args) -> HashMap<String, String> {
    replication_info(cfg)
}

fn simple_pattern_match(pattern: &str, key: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.starts_with("*") && pattern.ends_with("*") {
        return key.contains(&pattern[1..pattern.len() - 1]);
    }
    if pattern.starts_with("*") {
        return key.ends_with(&pattern[1..]);
    }
    if pattern.ends_with("*") {
        return key.starts_with(&pattern[..pattern.len() - 1]);
    }
    false
}

#[derive(Debug)]
struct StreamManager {
    stream_notify_tx: broadcast::Sender<()>,
}

impl Default for StreamManager {
    fn default() -> Self {
        Self {
            stream_notify_tx: broadcast::Sender::new(64),
        }
    }
}
