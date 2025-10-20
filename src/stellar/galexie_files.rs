pub struct GalexieFiles {
    ledgers_per_file: u32,
    files_per_partition: u32,
    extension: String,
    next_start: u32,
    end: Option<u32>,
} 

impl GalexieFiles {
    pub fn new(
        ledgers_per_file: u32,
        files_per_partition: u32, 
        extension: Option<String>,
        start: Option<u32>, 
        end: Option<u32>
    ) -> Self {
        let start = start.unwrap_or(0);

        GalexieFiles{
            ledgers_per_file,
            files_per_partition,
            extension: extension.unwrap_or("xdr.zstd".to_string()),
            next_start: (start / ledgers_per_file) * ledgers_per_file,
            end
        }
    }

    pub fn file_for_ledger(&self, ledger: u32) -> String {
        let ledgers_per_partition = self.ledgers_per_file * self.files_per_partition;
        let partition_start = (ledger / ledgers_per_partition) * ledgers_per_partition;
        let partition_end = partition_start + ledgers_per_partition - 1;
        let file_start = (ledger / self.ledgers_per_file) * self.ledgers_per_file;
        let file_end = file_start + self.ledgers_per_file - 1;
        format!(
            "{:X}--{:?}-{:?}/{:X}--{:?}-{:?}.{}",
            u32::MAX - partition_start,
            partition_start,
            partition_end,
            u32::MAX - file_start,
            file_start,
            file_end,
            self.extension
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
        self.next_start += self.ledgers_per_file;
        Some(self.file_for_ledger(res))
    }
}