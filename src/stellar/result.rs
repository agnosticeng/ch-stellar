use std::fmt::Debug;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, StellarError>;

#[derive(Error, Debug)]
pub enum StellarError {
    #[error("objectstore error")]
    ObjectStore(#[from] object_store::Error),
    #[error("i/o error")]
    IO(#[from] std::io::Error),
    #[error("URL parsing error")]
    URLParse(#[from] url::ParseError),
    #[error("XDR error")]
    XDRError(#[from] stellar_xdr::curr::Error),
    #[error("wrong XDR type")]
    WrongXDRType,
    #[error("empty Galexie datalake")]
    EmptyGalexieDataLake,
    #[error("empty network passphrase")]
    EmptyNetworkPassphrase,
}
