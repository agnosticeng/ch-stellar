use super::GalexieFiles;
use super::LedgerCloseMetaExt;
use super::query_params_ext::QueryParamsExt;
use super::result::{Result, StellarError};
use super::utils::build_opts;
use bytes::{Buf, Bytes};
use futures::stream::{self, StreamExt};
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use object_store::parse_url_opts;
use std::future;
use std::io::Read;
use std::str;
use std::sync::Arc;
use stellar_xdr::next::{LedgerCloseMeta, Limited, Limits, Type, TypeVariant};
use stream_flatten_iters::TryStreamExt as _;
use tokio::time::{Duration, sleep};
use url::Url;
use zstd::stream::read::Decoder;

pub const DEFAULT_XDR_RW_DEPTH_LIMIT: u32 = 500;
pub const DEFAULT_XDR_RW_LEN_LIMIT: usize = 1024 * 1024 * 32;

pub fn galexie_ledgers<'a>(
    base_url: &str,
    start: Option<u32>,
    end: Option<u32>,
) -> Result<impl Stream<Item = Result<Box<LedgerCloseMeta>>> + 'a> {
    let u = Arc::new(Url::parse(base_url)?);
    let opts = Arc::new(build_opts(&u));
    let opts1 = opts.clone();

    let it = GalexieFiles::new(
        opts.get_or_default("ledgers_per_file", 64),
        opts.get_or_default("files_per_partition", 1024),
        Some(opts.get_or_default("extension", "xdr.zstd".to_string())),
        start,
        end,
    );

    Ok(stream::iter(it)
        .map(move |file_path| {
            let u = u.clone();
            let opts = opts.clone();
            async move { download_file(u.as_ref(), &file_path, opts.as_ref()).await }
        })
        .buffered(opts1.get_or_default("max_concurrent_requests", 3))
        .and_then(decompress_file)
        .and_then(decode_xdr)
        .try_flatten_iters()
        .try_filter(move |l| {
            let ledger_seq = l.ledger_seq();
            future::ready(ledger_seq >= start.unwrap_or(1) && ledger_seq <= end.unwrap_or(u32::MAX))
        }))
}

pub async fn download_file(
    base_url: &Url,
    file_path: &str,
    opts: &[(String, String)],
) -> Result<Bytes> {
    let u = base_url.clone().join(file_path)?;
    let (objstr, path) = parse_url_opts(&u, opts.to_owned())?;
    Ok(objstr.get(&path).await?.bytes().await?)
}

pub async fn check_file_exists(
    base_url: &Url,
    file_path: &str,
    opts: &[(String, String)],
) -> Result<bool> {
    loop {
        let u = base_url.clone().join(file_path)?;
        let (objstr, path) = parse_url_opts(&u, opts.to_owned())?;

        match objstr.head(&path).await {
            Ok(_) => return Ok(true),
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => {
                if e.to_string().contains("429") {
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                } else {
                    return Err(StellarError::ObjectStore(e));
                }
            }
        }
    }
}

async fn decompress_file(data: Bytes) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    Decoder::new(data.reader())?.read_to_end(&mut buf)?;
    Ok(buf)
}

async fn decode_xdr(data: Vec<u8>) -> Result<Vec<Box<LedgerCloseMeta>>> {
    let mut xdr_reader = Limited::new(data.reader(), Limits::depth(DEFAULT_XDR_RW_DEPTH_LIMIT));

    Type::read_xdr_iter(TypeVariant::LedgerCloseMeta, &mut xdr_reader)
        .map_ok(decode_ledger_close_meta)
        .flatten()
        .collect::<Result<Vec<Box<LedgerCloseMeta>>>>()
}

fn decode_ledger_close_meta(t: Type) -> Result<Box<LedgerCloseMeta>> {
    match t {
        Type::LedgerCloseMeta(md) => Ok(md),
        _ => Err(StellarError::WrongXDRType),
    }
}
