use std::str;
use std::io::Read;
use std::future;
use std::sync::Arc;
use bytes::{Bytes,Buf};
use futures::{TryStreamExt,Stream};
use itertools::Itertools;
use object_store::{parse_url_opts};
use url::Url;
use futures::stream::{self,StreamExt};
use stream_flatten_iters::TryStreamExt as _;
use stellar_xdr::next::{LedgerCloseMeta, Limits, Type, TypeVariant, Limited};
use zstd::stream::read::Decoder;
use querystring::{querify};
use super::result::{Result,StellarError};
use super::GalexieFiles;
use super::LedgerCloseMetaExt;
use super::query_params_ext::QueryParamsExt;

pub const DEFAULT_XDR_RW_DEPTH_LIMIT: u32 = 500;

pub fn galexie_ledgers(
    base_url: &str, 
    start: Option<u32>, 
    end: Option<u32>
) -> Result<impl Stream<Item = Result<Box<LedgerCloseMeta>>>> {
    let u = Arc::new(Url::parse(base_url)?);
    let mut opts: Vec<(String,String)> = Vec::new();

    for (k, v) in std::env::vars() {
        if k.starts_with("AWS_") {
            opts.push((k.to_lowercase(), v));
        }
    }

    for (k, v) in querify(u.fragment().unwrap_or_default()) {
        opts.push((k.to_string(), v.to_string()));
    }

    let opts = Arc::new(opts);
    let opts1 = opts.clone();

    let it = GalexieFiles::new(
        opts.get_or_default("ledgers_per_file", 64),
        opts.get_or_default("files_per_partition", 1024),
        Some(opts.get_or_default("extension", "xdr.zstd".to_string())),
        start,
        end
    );

    Ok(
        stream::iter(it)
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
            })
    )
}

async fn download_file(base_url: &Url, file_path: &str, opts: &[(String, String)]) -> Result<Bytes> {
    let u = base_url.clone().join(file_path)?;
    let (objstr, path) = parse_url_opts(&u, opts.to_owned())?;
    Ok(objstr.get(&path).await?.bytes().await?)
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
        _ => Err(StellarError::WrongXDRType)
    }
}
