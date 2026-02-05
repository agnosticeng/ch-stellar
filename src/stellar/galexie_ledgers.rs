use crate::stellar::StellarError;

use super::GalexieFiles;
use super::LedgerCloseMetaExt;
use super::query_params_ext::QueryParamsExt;
use super::result::Result;
use async_compression::tokio::bufread::ZstdDecoder;
use ch_udf_common::object_store::{opts_from_env, opts_from_query_string};
use futures::stream::{self, StreamExt};
use futures::{Stream, TryStreamExt};
use object_store::parse_url_opts;
use std::future;
use std::sync::Arc;
use stellar_xdr::curr::ReadXdr;
use stellar_xdr::curr::{LedgerCloseMeta, Limited, Limits};
use tokio_util::io::StreamReader;
use tokio_util::io::SyncIoBridge;
use url::Url;

pub const DEFAULT_XDR_RW_DEPTH_LIMIT: u32 = 500;
pub const DEFAULT_XDR_RW_LEN_LIMIT: usize = 1024 * 1024 * 32;

pub fn galexie_ledgers<'a>(
    base_url: &str,
    start: Option<u32>,
    end: Option<u32>,
) -> Result<impl Stream<Item = Result<LedgerCloseMeta>> + 'a + use<'a>> {
    let mut u = Url::parse(base_url)?;
    let opts = itertools::concat([
        opts_from_env(),
        opts_from_query_string(u.fragment().unwrap_or("")),
    ]);
    u.set_fragment(None);
    let u = Arc::new(u);

    let it = GalexieFiles::new(
        opts.get_or_default("ledgers_per_file", 64),
        opts.get_or_default("files_per_partition", 1024),
        Some(opts.get_or_default("extension", "xdr.zstd".to_string())),
        start,
        end,
    );

    Ok(stream::iter(it)
        .map({
            let opts = opts.clone();
            move |file_path| {
                let u = u.clone();
                let opts = opts.clone();
                async move { process_file(u.as_ref(), &file_path, opts.as_ref()).await }
            }
        })
        .buffered(opts.clone().get_or_default("max_concurrent_requests", 3))
        .try_flatten()
        .try_filter(move |l| {
            let ledger_seq = l.ledger_seq();
            future::ready(ledger_seq >= start.unwrap_or(1) && ledger_seq <= end.unwrap_or(u32::MAX))
        }))
}

async fn process_file(
    base_url: &Url,
    file_path: &str,
    opts: &[(String, String)],
) -> Result<impl Stream<Item = Result<LedgerCloseMeta>> + use<>> {
    let u = base_url.clone().join(file_path)?;
    let (objstr, path) = parse_url_opts(&u, opts.to_owned())?;

    let response = objstr.get(&path).await?;
    let byte_stream = response.into_stream();

    let reader = StreamReader::new(byte_stream.map_err(std::io::Error::other));
    let decoder = ZstdDecoder::new(reader);
    let bridge = SyncIoBridge::new(decoder);

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<LedgerCloseMeta>>(64);

    tokio::task::spawn_blocking(move || {
        let mut xdr_reader = Limited::new(bridge, Limits::depth(DEFAULT_XDR_RW_DEPTH_LIMIT));

        let len = match read_ledger_close_meta_batch_len(&mut xdr_reader) {
            Ok(len) => len,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                return;
            }
        };

        for _ in 0..len {
            match LedgerCloseMeta::read_xdr(&mut xdr_reader) {
                Ok(lcm) => {
                    if tx.blocking_send(Ok(lcm)).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.blocking_send(Err(e.into()));
                    break;
                }
            }
        }
    });

    Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
}

fn read_ledger_close_meta_batch_len<R>(r: &mut Limited<R>) -> Result<u32>
where
    R: std::io::Read,
{
    // Read batch header fields individually instead of the whole batch,
    // so we can stream each LedgerCloseMeta as it's parsed
    u32::read_xdr(r)?; // start_sequence
    u32::read_xdr(r)?; // end_sequence
    u32::read_xdr(r).map_err(StellarError::from) // array length
}
