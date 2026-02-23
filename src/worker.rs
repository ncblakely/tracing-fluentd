use core::{mem, time};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{sync_channel, SyncSender},
    },
    time::Duration,
};

use crossbeam_channel::SendTimeoutError;

use crate::{fluent, MakeWriter};

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
        let mut ongoing_writer = None;

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

            match rmp_serde::encode::write(&mut writer, &msg) {
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

                if let Err(error) = rmp_serde::encode::write(&mut writer, &msg) {
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
