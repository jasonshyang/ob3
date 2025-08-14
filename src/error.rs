use thiserror::Error;

use crate::types::Command;

#[derive(Error, Debug)]
pub enum Error<T> {
    #[error("Try Send Error: {0}")]
    TrySendError(#[from] crossbeam_channel::TrySendError<Command<T>>),
    #[error("Send Error: {0}")]
    SendError(#[from] crossbeam_channel::SendError<Command<T>>),
    #[error("Couldn't join on the associated thread")]
    JoinError,
}
