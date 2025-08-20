use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Send Error: {0}")]
    SendError(String),
    #[error("Couldn't join on the associated thread")]
    JoinError,
    #[error("Order not found: {0}")]
    OrderNotFound(String),
    #[error("Already shutdown")]
    AlreadyShutdown,
}

impl<T> From<crossbeam_channel::SendError<T>> for Error {
    fn from(err: crossbeam_channel::SendError<T>) -> Self {
        Error::SendError(err.to_string())
    }
}

impl<T> From<crossbeam_channel::TrySendError<T>> for Error {
    fn from(err: crossbeam_channel::TrySendError<T>) -> Self {
        match err {
            crossbeam_channel::TrySendError::Full(_) => {
                Error::SendError("Channel is full".to_string())
            }
            crossbeam_channel::TrySendError::Disconnected(_) => {
                Error::SendError("Channel disconnected".to_string())
            }
        }
    }
}
