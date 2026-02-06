use super::GalexieFiles;
use super::LedgerCloseMetaExt;
use super::StellarError;
use super::constants::DEFAULT_XDR_RW_DEPTH_LIMIT;
use super::galexie_config::GalexieConfig;
use super::result::Result;
use async_compression::tokio::bufread::ZstdDecoder;
use ch_udf_common::object_store::{opts_from_env, opts_from_query_string};
use futures::stream::{self, StreamExt};
use futures::{Stream, TryStreamExt};
use object_store::{ObjectStore, parse_url_opts, path::Path};
use std::future;
use std::sync::Arc;
use stellar_xdr::curr::ReadXdr;
use stellar_xdr::curr::{LedgerCloseMeta, Limited, Limits};
use tokio_util::io::StreamReader;
use tokio_util::io::SyncIoBridge;
use url::Url;

pub struct Galexie {
    objstr: Box<dyn ObjectStore>,
    base_path: Path,
    config: GalexieConfig,
}

impl Galexie {
    pub async fn new(base_url: &str) -> Result<Self> {
        let mut u = Url::parse(base_url)?;
        let opts = itertools::concat([
            opts_from_env(),
            opts_from_query_string(u.fragment().unwrap_or("")),
        ]);
        u.set_fragment(None);

        let (objstr, base_path) = parse_url_opts(&u, opts)?;
        let config_path = base_path.child(".config.json");

        let bytes = objstr.get(&config_path).await?.bytes().await?;
        let config: GalexieConfig = serde_json::from_slice(&bytes)?;

        Ok(Self {
            objstr,
            base_path,
            config,
        })
    }

    pub fn network_passphrase(&self) -> &str {
        &self.config.network_passphrase
    }

    pub fn ledgers(
        self: Arc<Self>,
        start: Option<u32>,
        end: Option<u32>,
    ) -> Result<impl Stream<Item = Result<LedgerCloseMeta>>> {
        let it = GalexieFiles::new(self.config.clone(), start, end);

        Ok(stream::iter(it)
            .map({
                let this = Arc::clone(&self);
                move |file_path| {
                    let this = Arc::clone(&this);
                    let full_path = Path::parse(format!("{}/{}", self.base_path, file_path))
                        .expect("failed to build full path");
                    async move { this.process_file(&full_path).await }
                }
            })
            .buffered(4)
            .try_flatten()
            .try_filter(move |l| {
                let ledger_seq = l.ledger_seq();
                future::ready(
                    ledger_seq >= start.unwrap_or(1) && ledger_seq <= end.unwrap_or(u32::MAX),
                )
            }))
    }

    async fn process_file(
        &self,
        file_path: &Path,
    ) -> Result<impl Stream<Item = Result<LedgerCloseMeta>> + use<>> {
        let response = self.objstr.get(file_path).await?;
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
