#[derive(Debug, Clone, serde::Deserialize)]
pub struct GalexieConfig {
    #[serde(alias = "networkPassphrase")]
    pub network_passphrase: String,
    #[serde(alias = "version")]
    pub version: String,
    #[serde(alias = "compression")]
    pub compression: Compression,
    #[serde(alias = "ledgersPerBatch")]
    pub ledgers_per_batch: u32,
    #[serde(alias = "batchesPerPartition")]
    pub batches_per_partition: u32,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub enum Compression {
    #[serde(alias = "", alias = "none")]
    None,
    #[serde(alias = "zstd")]
    Zstd,
}

impl Compression {
    pub fn extension(&self) -> &'static str {
        match self {
            Compression::None => "",
            Compression::Zstd => ".zst",
        }
    }
}
