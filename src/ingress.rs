use std::{thread::JoinHandle, time::Duration};

use crossbeam_channel::Sender;

use crate::{
    error::Error,
    types::{BackPressureStrategy, BatchProcessor, Command, Query},
};

/*
    Generic Ingress module for processing commands in batches.
*/

#[derive(Debug, Clone)]
pub struct IngressConfig {
    pub query_channel_size: usize,
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

    pub fn send(&self, command: Command<T>) -> Result<(), Error> {
        match self.config.back_pressure_strategy {
            BackPressureStrategy::Block => self.sender.send(command)?, // Block until the command is sent
            BackPressureStrategy::Drop => match self.sender.try_send(command) {
                Ok(_) | Err(crossbeam_channel::TrySendError::Full(_)) => {}
                Err(e) => return Err(Error::SendError(e.to_string())),
            },
        }

        Ok(())
    }
}

impl<T> Controller<T> {
    pub fn shutdown(&mut self) -> Result<(), Error> {
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
) -> (Producer<T>, Controller<T>, Sender<Query<P::Snapshot>>)
where
    P: BatchProcessor<Operation = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded(config.buffer_size);
    let (query_tx, query_rx) = crossbeam_channel::bounded(config.query_channel_size);
    let producer = Producer::new(tx.clone(), config.clone());

    let handle = std::thread::spawn(move || {
        let mut batch = Vec::with_capacity(config.batch_size);
        let mut last_flush = std::time::Instant::now();
        loop {
            crossbeam_channel::select! {
                recv(rx) -> command => match command {
                    Ok(Command::Shutdown) => {
                        if !batch.is_empty() {
                            processor.process_ops(std::mem::take(&mut batch));
                        }
                        break;
                    }
                    Ok(Command::Operation(op)) => {
                        batch.push(op);
                        if batch.len() >= config.batch_size {
                            processor.process_ops(std::mem::take(&mut batch));
                            last_flush = std::time::Instant::now();
                        }
                    }
                    Err(crossbeam_channel::RecvError) => break,
                },
                recv(query_rx) -> query => match query {
                    Ok(q) => {
                        if let Err(e) = processor.process_query(q) {
                            eprintln!("Error processing query: {}", e);
                        }
                    }
                    Err(crossbeam_channel::RecvError) => break,
                }
            }

            if last_flush.elapsed() >= config.max_delay_ms && !batch.is_empty() {
                processor.process_ops(std::mem::take(&mut batch));
                last_flush = std::time::Instant::now();
            }
        }
    });

    let controller = Controller {
        sender: tx,
        handle: Some(handle),
    };

    (producer, controller, query_tx)
}
