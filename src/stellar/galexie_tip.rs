use url::Url;
use super::result::Result;
use super::GalexieFiles;
use super::query_params_ext::QueryParamsExt;
use super::galexie_ledgers::check_file_exists;
use super::utils::build_opts;

pub async fn galexie_tip(base_url: &str, low_hint: Option<u32>, high_hint: Option<u32>) -> Result<u32>{
    let u = Url::parse(base_url)?;
    let opts = build_opts(&u);

    let it = GalexieFiles::new(
        opts.get_or_default("ledgers_per_file", 64),
        opts.get_or_default("files_per_partition", 1024),
        Some(opts.get_or_default("extension", "xdr.zstd".to_string())),
        None,
        None
    );

    let mut low: u32 = low_hint.unwrap_or(1);
    let mut high: u32 = high_hint.unwrap_or(u32::MAX);
    let mut res: u32 = 0;

    while low <= high {
        let i = low + ((high-low)/2);

        if i == low || i == high {
            break
        }

        let f = it.file_for_ledger(i);
        let file_exists = check_file_exists(&u, &f, &opts).await?;

        eprintln!("{:?} ({:?}/{:?}): {}", i, low, high, file_exists);

        if file_exists {
            res = i;
            low = i;
        } else {
            res = i;
            high = i;
        }
    }

    Ok(res)
}