use super::galexie_config::GalexieConfig;

pub struct GalexieFiles {
    conf: GalexieConfig,
    next_start: u32,
    end: Option<u32>,
}

impl GalexieFiles {
    pub fn new(conf: GalexieConfig, start: Option<u32>, end: Option<u32>) -> Self {
        let start = start.unwrap_or(0);

        GalexieFiles {
            next_start: (start / conf.ledgers_per_batch) * conf.ledgers_per_batch,
            end,
            conf,
        }
    }

    pub fn file_for_ledger(&self, ledger: u32) -> String {
        let ledgers_per_partition = self.conf.ledgers_per_batch * self.conf.batches_per_partition;
        let partition_start = (ledger / ledgers_per_partition) * ledgers_per_partition;
        let partition_end = partition_start + ledgers_per_partition - 1;
        let file_start = (ledger / self.conf.ledgers_per_batch) * self.conf.ledgers_per_batch;
        let file_end = file_start + self.conf.ledgers_per_batch - 1;
        let extension = self.conf.compression.extension();

        format!(
            "{:X}--{:?}-{:?}/{:X}--{:?}-{:?}.xdr{}",
            u32::MAX - partition_start,
            partition_start,
            partition_end,
            u32::MAX - file_start,
            file_start,
            file_end,
            extension
        )
    }
}

impl Iterator for GalexieFiles {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_start > self.end.unwrap_or(u32::MAX) {
            return None;
        }

        let res = self.next_start;
        self.next_start += self.conf.ledgers_per_batch;
        Some(self.file_for_ledger(res))
    }
}
