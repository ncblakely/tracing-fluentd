use core::{mem, time};
use std::{
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{sync_channel, SyncSender},
    },
    time::Duration,
};

use crossbeam_channel::SendTimeoutError;

use crate::{fluent, MakeWriter};

#[cfg(feature = "callsite_stats")]
use std::{collections::HashMap, fmt::Write as FmtWrite, fs, time::Instant};

#[cfg(feature = "callsite_stats")]
#[derive(Default)]
struct CallsiteStats {
    records: u64,
    estimated_payload_bytes: u64,
}

#[cfg(feature = "callsite_stats")]
fn update_callsite_stats(
    msg: &fluent::Message,
    stats: &mut HashMap<String, CallsiteStats>,
    total_records: &mut u64,
    total_estimated_payload_bytes: &mut u64,
) {
    for record in msg.records() {
        let estimated_payload_bytes = record.estimated_payload_bytes() as u64;
        *total_records += 1;
        *total_estimated_payload_bytes += estimated_payload_bytes;

        let callsite = record
            .callsite_key()
            .unwrap_or_else(|| "<unknown-callsite>".to_owned());
        let entry = stats.entry(callsite).or_default();
        entry.records += 1;
        entry.estimated_payload_bytes += estimated_payload_bytes;
    }
}

#[cfg(not(feature = "callsite_stats"))]
fn update_callsite_stats(
    _msg: &fluent::Message,
    _stats: &mut (),
    _total_records: &mut u64,
    _total_estimated_payload_bytes: &mut u64,
) {
}

#[cfg(feature = "callsite_stats")]
fn write_callsite_stats(
    stats_path: &str,
    stats: &HashMap<String, CallsiteStats>,
    total_records: u64,
    total_estimated_payload_bytes: u64,
) -> std::io::Result<()> {
    let mut rows: Vec<_> = stats.iter().collect();
    rows.sort_by(|left, right| {
        right
            .1
            .estimated_payload_bytes
            .cmp(&left.1.estimated_payload_bytes)
            .then_with(|| right.1.records.cmp(&left.1.records))
            .then_with(|| left.0.cmp(right.0))
    });

    let mut output = String::new();
    let _ = writeln!(output, "# total_records={total_records}");
    let _ = writeln!(
        output,
        "# total_estimated_payload_bytes={total_estimated_payload_bytes}"
    );
    let _ = writeln!(
        output,
        "estimated_share_pct\testimated_payload_bytes\trecords\tcallsite"
    );

    for (callsite, stat) in rows.into_iter().take(100) {
        let share_pct = if total_estimated_payload_bytes == 0 {
            0.0
        } else {
            (stat.estimated_payload_bytes as f64 * 100.0) / total_estimated_payload_bytes as f64
        };
        let _ = writeln!(
            output,
            "{share_pct:.4}\t{}\t{}\t{}",
            stat.estimated_payload_bytes, stat.records, callsite
        );
    }

    let temp_path = format!("{stats_path}.tmp");
    fs::write(&temp_path, output)?;
    fs::rename(temp_path, stats_path)
}

#[cfg(feature = "callsite_stats")]
fn flush_callsite_stats(
    stats_path: &str,
    stats: &HashMap<String, CallsiteStats>,
    total_records: u64,
    total_estimated_payload_bytes: u64,
    last_stats_write: &mut Instant,
) {
    if let Err(error) = write_callsite_stats(
        stats_path,
        stats,
        total_records,
        total_estimated_payload_bytes,
    ) {
        eprintln!(
            "tracing-fluentd: failed to write callsite stats to {}: {}",
            stats_path, error
        );
    } else {
        *last_stats_write = Instant::now();
    }
}

fn write_message<W: Write>(
    writer: &mut W,
    msg: &fluent::Message,
    encoded_msg: &mut Vec<u8>,
) -> Result<(), String> {
    encoded_msg.clear();
    rmp_serde::encode::write(encoded_msg, msg).map_err(|error| error.to_string())?;
    writer
        .write_all(encoded_msg.as_slice())
        .map_err(|error| error.to_string())?;
    writer.flush().map_err(|error| error.to_string())
}

pub enum Message {
    Record(fluent::Record),
    /// Flush all pending records immediately without terminating.
    Flush,
    /// Flush all pending records and notify completion via the channel.
    FlushSync(SyncSender<()>),
    Terminate,
}

impl Into<Message> for fluent::Record {
    #[inline(always)]
    fn into(self) -> Message {
        Message::Record(self)
    }
}

pub trait Consumer: 'static {
    fn record(&self, record: fluent::Record);
}

#[repr(transparent)]
pub struct WorkerChannel(pub(crate) crossbeam_channel::Sender<Message>);

impl Consumer for WorkerChannel {
    #[inline(always)]
    fn record(&self, record: fluent::Record) {
        let _ = self.0.send(record.into());
    }
}

pub struct ThreadWorker {
    sender: mem::ManuallyDrop<crossbeam_channel::Sender<Message>>,
    send_timeouts: AtomicUsize,
    worker: mem::ManuallyDrop<std::thread::JoinHandle<()>>,
    send_timeout: Option<Duration>,
}

impl ThreadWorker {
    #[inline(always)]
    pub fn sender(&self) -> crossbeam_channel::Sender<Message> {
        mem::ManuallyDrop::into_inner(self.sender.clone())
    }

    #[inline(always)]
    pub fn stop(&self) {
        let _result = self.sender.send(Message::Terminate);
        debug_assert!(_result.is_ok());
    }

    /// Sends a flush signal to the worker, causing it to immediately send
    /// all pending records without terminating.
    #[inline(always)]
    pub fn flush(&self) {
        let _ = self.sender.send(Message::Flush);
    }

    /// Sends a flush signal and blocks until the worker has finished sending
    /// all pending records, or until `timeout` elapses.
    ///
    /// Returns `true` if the flush completed within the timeout.
    pub fn flush_blocking(&self, timeout: Duration) -> bool {
        let (tx, rx) = sync_channel(0);
        let _ = self.sender.send(Message::FlushSync(tx));
        rx.recv_timeout(timeout).is_ok()
    }
}

impl Consumer for ThreadWorker {
    #[inline(always)]
    fn record(&self, record: fluent::Record) {
        if let Some(send_timeout) = self.send_timeout {
            match self.sender.send_timeout(record.into(), send_timeout) {
                Err(SendTimeoutError::Timeout(_)) => {
                    self.send_timeouts.fetch_add(1, Ordering::Relaxed);
                }
                Err(SendTimeoutError::Disconnected(_)) => return,
                Ok(_) => {}
            }
        } else {
            let _ = self.sender.send(record.into());
        }
    }
}

impl Drop for ThreadWorker {
    fn drop(&mut self) {
        let send_timeouts = self.send_timeouts.load(Ordering::Relaxed);
        if send_timeouts > 0 {
            tracing::event!(
                tracing::Level::WARN,
                "Fluent worker encountered {} send timeouts",
                send_timeouts
            );
        }

        let worker = unsafe {
            mem::ManuallyDrop::drop(&mut self.sender);
            mem::ManuallyDrop::take(&mut self.worker)
        };
        //Since we're dropping then probably application is terminating
        //or logger is removed, so no one would receive event
        let _ = worker.join();
    }
}

pub fn thread<MW: MakeWriter>(
    tag: &'static str,
    writer: MW,
    max_msg_record: usize,
    channel_capacity: Option<usize>,
    channel_timeout: Option<Duration>,
) -> std::io::Result<ThreadWorker> {
    //const MAX_WAIT: time::Duration = time::Duration::from_secs(60);

    let (sender, recv) = if let Some(channel_capacity) = channel_capacity {
        crossbeam_channel::bounded(channel_capacity)
    } else {
        crossbeam_channel::unbounded()
    };

    let worker = std::thread::Builder::new().name("tracing-fluentd-worker".to_owned());

    let worker = worker.spawn(move || {
        let mut msg = fluent::Message::new(tag);
        let mut encoded_msg = Vec::with_capacity(64 * 1024);
        let mut ongoing_writer = None;
        #[cfg(feature = "callsite_stats")]
        let mut callsite_stats = HashMap::<String, CallsiteStats>::new();
        #[cfg(not(feature = "callsite_stats"))]
        let mut callsite_stats = ();
        let mut total_records = 0_u64;
        let mut total_estimated_payload_bytes = 0_u64;
        #[cfg(feature = "callsite_stats")]
        let mut last_stats_write = Instant::now();
        #[cfg(not(feature = "callsite_stats"))]
        let _last_stats_write = ();
        #[cfg(feature = "callsite_stats")]
        let callsite_stats_path = std::env::var("TRACING_FLUENTD_CALLSITE_STATS_PATH")
            .unwrap_or_else(|_| {
                if fs::metadata("/tmp/ice-profiles").is_ok() {
                    "/tmp/ice-profiles/tracing-fluentd-callsite-stats.tsv".to_owned()
                } else {
                    "tracing-fluentd-callsite-stats.tsv".to_owned()
                }
            });
        #[cfg(not(feature = "callsite_stats"))]
        let _callsite_stats_path = "";

        'main_loop: loop {
            let mut flush_notify: Option<SyncSender<()>> = None;

            //Fetch up to max_msg_record
            while msg.len() < max_msg_record {
                match recv.recv() {
                    Ok(Message::Record(record)) => msg.add(record),
                    Ok(Message::Flush) => break,
                    Ok(Message::FlushSync(notify)) => {
                        flush_notify = Some(notify);
                        break;
                    }
                    Ok(Message::Terminate) | Err(crossbeam_channel::RecvError) => break 'main_loop,
                }
            }

            //Get every extra record we can get at the current moment.
            loop {
                match recv.try_recv() {
                    Ok(Message::Record(record)) => msg.add(record),
                    Ok(Message::Flush) | Err(crossbeam_channel::TryRecvError::Empty) => break,
                    Ok(Message::FlushSync(notify)) => {
                        flush_notify = Some(notify);
                        break;
                    }
                    Ok(Message::Terminate) | Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        break 'main_loop
                    }
                }
            }

            let mut writer = match ongoing_writer.take() {
                Some(writer) => writer,
                None => match writer.make() {
                    Ok(writer) => writer,
                    Err(_) => {
                        std::thread::sleep(time::Duration::from_secs(1));
                        match writer.make() {
                            Ok(writer) => writer,
                            Err(error) => {
                                tracing::event!(
                                    tracing::Level::DEBUG,
                                    "Failed to create fluent writer {}",
                                    error
                                );
                                continue 'main_loop;
                            }
                        }
                    }
                },
            };

            update_callsite_stats(
                &msg,
                &mut callsite_stats,
                &mut total_records,
                &mut total_estimated_payload_bytes,
            );
            #[cfg(feature = "callsite_stats")]
            if flush_notify.is_some() || last_stats_write.elapsed() >= Duration::from_secs(1) {
                flush_callsite_stats(
                    &callsite_stats_path,
                    &callsite_stats,
                    total_records,
                    total_estimated_payload_bytes,
                    &mut last_stats_write,
                );
            }

            match write_message(&mut writer, &msg, &mut encoded_msg) {
                Ok(()) => {
                    msg.clear();
                    ongoing_writer = Some(writer);
                }
                //In case of error we'll just retry at later date.
                //Ideally we should be able to recover.
                //But report error?
                Err(error) => {
                    tracing::event!(
                        tracing::Level::INFO,
                        "Failed to send records to fluent server {}",
                        error
                    );
                }
            }

            if let Some(notify) = flush_notify {
                let _ = notify.send(());
            }
        }

        if msg.len() > 0 {
            //Try to flush last records, but don't wait too much
            for _ in 0..3 {
                let mut writer = match ongoing_writer.take() {
                    Some(writer) => writer,
                    None => match writer.make() {
                        Ok(writer) => writer,
                        Err(_) => {
                            std::thread::sleep(time::Duration::from_secs(1));
                            match writer.make() {
                                Ok(writer) => writer,
                                Err(error) => {
                                    tracing::event!(
                                        tracing::Level::DEBUG,
                                        "Failed to create fluent writer {}",
                                        error
                                    );
                                    continue;
                                }
                            }
                        }
                    },
                };

                update_callsite_stats(
                    &msg,
                    &mut callsite_stats,
                    &mut total_records,
                    &mut total_estimated_payload_bytes,
                );
                #[cfg(feature = "callsite_stats")]
                flush_callsite_stats(
                    &callsite_stats_path,
                    &callsite_stats,
                    total_records,
                    total_estimated_payload_bytes,
                    &mut last_stats_write,
                );

                if let Err(error) = write_message(&mut writer, &msg, &mut encoded_msg) {
                    tracing::event!(
                        tracing::Level::INFO,
                        "Failed to send last records to fluent server {}",
                        error
                    );
                    std::thread::sleep(time::Duration::from_secs(1));
                } else {
                    break;
                }
            }
        }
    })?;

    Ok(ThreadWorker {
        sender: mem::ManuallyDrop::new(sender),
        send_timeout: channel_timeout,
        send_timeouts: AtomicUsize::new(0),
        worker: mem::ManuallyDrop::new(worker),
    })
}
