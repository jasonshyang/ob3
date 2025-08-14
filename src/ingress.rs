use std::{thread::JoinHandle, time::Duration};

use crossbeam_channel::Sender;

use crate::{
    error::Error,
    types::{BackPressureStrategy, BatchProcessor, Command},
};

/*
    Generic Ingress module for processing commands in batches.
*/

#[derive(Debug, Clone)]
pub struct IngressConfig {
    pub max_delay_ms: Duration,
    pub buffer_size: usize,
    pub batch_size: usize,
    pub back_pressure_strategy: BackPressureStrategy,
}

#[derive(Debug, Clone)]
pub struct Producer<T> {
    sender: Sender<Command<T>>,
    config: IngressConfig,
}

#[derive(Debug)]
pub struct Controller<T> {
    handle: Option<JoinHandle<()>>,
    sender: Sender<Command<T>>,
}

impl<T> Producer<T> {
    pub fn new(sender: Sender<Command<T>>, config: IngressConfig) -> Self {
        Self { sender, config }
    }

    pub fn send(&self, command: Command<T>) -> Result<(), Error<T>> {
        match self.config.back_pressure_strategy {
            BackPressureStrategy::Block => self.sender.send(command)?, // Block until the command is sent
            BackPressureStrategy::Drop => match self.sender.try_send(command) {
                Ok(_) | Err(crossbeam_channel::TrySendError::Full(_)) => {}
                Err(e) => return Err(Error::TrySendError(e)),
            },
        }

        Ok(())
    }
}

impl<T> Controller<T> {
    pub fn shutdown(&mut self) -> Result<(), Error<T>> {
        if let Some(handle) = self.handle.take() {
            self.sender.send(Command::Shutdown)?;
            handle.join().map_err(|_| Error::JoinError)?;
        }
        Ok(())
    }
}

pub fn spawn_processor_thread<P, T>(
    mut processor: P,
    config: IngressConfig,
) -> (Producer<T>, Controller<T>)
where
    P: BatchProcessor<Operation = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded(config.buffer_size);
    let producer = Producer::new(tx.clone(), config.clone());
    let handle = std::thread::spawn(move || {
        // For now we only batch up the add commands
        let mut batch = Vec::with_capacity(config.batch_size);
        let mut last_flush = std::time::Instant::now();
        loop {
            let timeout = config.max_delay_ms.saturating_sub(last_flush.elapsed());
            match rx.recv_timeout(timeout) {
                Ok(Command::Shutdown) => {
                    let snapshot = processor.produce_snapshot();
                    println!("Final snapshot: {snapshot:#?}");
                    break;
                }
                Ok(Command::Operation(op)) => {
                    batch.push(op);
                    if batch.len() >= config.batch_size {
                        processor.process_ops(std::mem::take(&mut batch));
                        last_flush = std::time::Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !batch.is_empty() {
                        processor.process_ops(std::mem::take(&mut batch));
                        last_flush = std::time::Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    });

    let controller = Controller {
        sender: tx,
        handle: Some(handle),
    };

    (producer, controller)
}
