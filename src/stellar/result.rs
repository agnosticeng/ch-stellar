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
    #[error("wrong XDR type")]
    WrongXDRType
}