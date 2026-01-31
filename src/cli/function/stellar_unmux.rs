use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use clap::Args;
use core::str;
use std::io::{stdin, stdout};
use std::sync::Arc;
use stellar_strkey::{Strkey, ed25519};

#[derive(Debug, Clone, Args)]
pub struct StellarUnmuxCommand {}

impl StellarUnmuxCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Binary,
            false,
        )]));

        loop {
            let reader = StreamReader::try_new(stdin(), None)?;
            let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

            for input_batch in reader {
                let input_batch = input_batch.context("failed to read input batch")?;
                let mut result_col_builder = GenericByteBuilder::<BinaryType>::with_capacity(
                    input_batch.num_rows(),
                    input_batch.num_rows() * 1024,
                );
                let address_col: &BinaryArray = input_batch.get_column("address")?;

                let it = address_col.iter().map(|s| -> Result<String> {
                    let key = Strkey::from_string(str::from_utf8(s.unwrap())?)?;

                    match key {
                        Strkey::MuxedAccountEd25519(ed25519::MuxedAccount { ed25519, .. }) => {
                            Ok(stellar_strkey::ed25519::PublicKey(ed25519).to_string())
                        }
                        _ => Ok("".to_owned()),
                    }
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
