//! Best-effort supervisor diagnostics: output backpressure must never delay
//! lease decisions or process-group shutdown.
use std::{
    fs::File,
    io::{self, Write},
    os::fd::AsFd,
    sync::mpsc::{self, SyncSender},
    time::Duration,
};

enum Message {
    Line(String),
    Flush(SyncSender<()>),
}

pub(super) struct Diagnostics {
    sender: Option<SyncSender<Message>>,
}

impl Diagnostics {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::sync_channel(16);
        // Duplication owns a separate descriptor, with close-on-exec, while
        // retaining the inherited open-file flags unchanged. The worker uses
        // no Rust stderr lock that could strand another diagnostic producer.
        let Ok(fd) = io::stderr().as_fd().try_clone_to_owned() else {
            return Self { sender: None };
        };
        let worker = std::thread::Builder::new()
            .name("solo-diagnostics".into())
            .spawn(move || {
                let mut output = File::from(fd);
                while let Ok(message) = receiver.recv() {
                    match message {
                        Message::Line(line) => {
                            if output.write_all(line.as_bytes()).is_err() {
                                break;
                            }
                        }
                        Message::Flush(done) => {
                            let _ = done.try_send(());
                        }
                    }
                }
            });
        // Dropping a JoinHandle detaches it. Never join a potentially blocked
        // output writer, including on runtime shutdown or broken output.
        Self {
            sender: worker.ok().map(|_| sender),
        }
    }

    pub(super) fn emit(&self, mut line: String) {
        // Current callers use only fixed safe error categories and timestamps.
        // Keep queued storage bounded even if a future caller grows a message.
        let mut end = line.len().min(2048);
        while !line.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        line.truncate(end);
        line.push('\n');
        line.shrink_to(2049);
        if let Some(sender) = &self.sender {
            // Loss under backpressure is intentional: diagnostics are optional,
            // while child shutdown and lease ownership are safety-critical.
            let _ = sender.try_send(Message::Line(line));
        }
    }
}

impl Drop for Diagnostics {
    fn drop(&mut self) {
        let (done, completed) = mpsc::sync_channel(1);
        if self
            .sender
            .as_ref()
            .is_some_and(|sender| sender.try_send(Message::Flush(done)).is_ok())
        {
            // Ordinary one-shot commands retain their final diagnostics. A
            // full/stalled output pipe can spend only this fixed exit budget.
            let _ = completed.recv_timeout(Duration::from_millis(100));
        }
    }
}
