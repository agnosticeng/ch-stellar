#![feature(str_split_remainder)]

use super::GalexieFiles;
use super::LedgerCloseMetaExt;
use super::query_params_ext::QueryParamsExt;
use super::result::{Result, StellarError};
use bytes::{Buf, Bytes};
use ch_udf_common::object_store::{opts_from_env, opts_from_query_string};
use futures::pin_mut;
use futures::stream::{self, StreamExt};
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use object_store::parse_url_opts;
use regex::Regex;
use std::future;
use std::io::Read;
use std::str;
use std::sync::Arc;
use std::sync::OnceLock;
use stellar_xdr::curr::{LedgerCloseMeta, Limited, Limits, Type, TypeVariant};
use url::Url;

pub async fn galexie_tip(base_url: &str) -> Result<u32> {
    let mut u = Url::parse(base_url)?;
    let opts = itertools::concat([
        opts_from_env(),
        opts_from_query_string(&u.fragment().unwrap_or("")),
    ]);
    u.set_fragment(None);
    let u = Arc::new(u);
    let (objstr, path) = parse_url_opts(&u, opts.to_owned())?;

    let meta = objstr
        .list(Some(&path))
        .try_filter(|meta| future::ready(!meta.location.to_string().ends_with(".config.json")))
        .next()
        .await
        .ok_or(StellarError::EmptyGalexieDataLake)??;

    let filename = meta
        .location
        .filename()
        .ok_or(StellarError::WrongGalexieFilename(
            meta.location.to_string(),
        ))?;

    let tip = filename
        .split("-")
        .last()
        .ok_or(StellarError::WrongGalexieFilename(
            meta.location.to_string(),
        ))
        .and_then(|s| {
            s.split(".")
                .next()
                .ok_or(StellarError::WrongGalexieFilename(
                    meta.location.to_string(),
                ))
        })?
        .parse::<u32>()
        .map_err(|_| StellarError::WrongGalexieFilename(meta.location.to_string()))?;

    eprintln!("{:?}", meta.location);

    Ok(tip)
}
