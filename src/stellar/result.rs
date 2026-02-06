use std::fmt::Debug;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, StellarError>;

#[derive(Error, Debug)]
pub enum StellarError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    URLParse(#[from] url::ParseError),
    #[error(transparent)]
    JSON(#[from] serde_json::Error),
    #[error("XDR error")]
    XDRError(#[from] stellar_xdr::curr::Error),
    #[error("wrong XDR type")]
    WrongXDRType,
    #[error("empty Galexie datalake")]
    EmptyGalexieDataLake,
    #[error("empty network passphrase")]
    EmptyNetworkPassphrase,
    #[error("wrong galexie filename {0}")]
    WrongGalexieFilename(String),
    #[error("unmatched tx envelope")]
    UnmatchedTxEnvelope,
    #[error("invalid compression")]
    InvalidCompression,
}
