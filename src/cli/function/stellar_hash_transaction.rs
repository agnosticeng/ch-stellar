use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use clap::Args;
use itertools::izip;
use sha2::{Digest, Sha256};
use std::io::{stdin, stdout};
use std::sync::Arc;
use stellar_xdr::curr::TransactionEnvelope;

#[derive(Debug, Clone, Args)]
pub struct StellarHashTransactionCommand {}

impl StellarHashTransactionCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Binary,
            false,
        )]));

        loop {
            let reader = StreamReader::try_new_buffered(stdin(), None)?;
            let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

            for input_batch in reader {
                let input_batch = input_batch.context("failed to read input batch")?;

                let mut result_col_builder = GenericByteBuilder::<BinaryType>::with_capacity(
                    input_batch.num_rows(),
                    input_batch.num_rows() * 1024,
                );

                let data_col: &BinaryArray = input_batch.get_column("data")?;
                let passphrase_col: &BinaryArray = input_batch.get_column("passphrase")?;

                let it =
                    izip!(data_col, passphrase_col).map(|(data, passphrase)| -> Result<String> {
                        let envelope: TransactionEnvelope =
                            serde_json::from_slice(data.unwrap())
                                .context("failed to deserialize transaction envelope")?;
                        let network_id: [u8; 32] =
                            Sha256::digest(passphrase.as_ref().unwrap()).into();
                        let hash = envelope.hash(network_id)?;
                        Ok(hex::encode(hash))
                    });

                for v in it {
                    result_col_builder.append_value(v?);
                }

                let result_col = result_col_builder.finish();
                let output_batch =
                    RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result_col)])?;
                writer
                    .write(&output_batch)
                    .context("failed to write output batch")?;
                writer.flush().context("failed to flush output stream")?;
            }
        }
    }
}
