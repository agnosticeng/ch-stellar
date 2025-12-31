use anyhow::{Context, Result};
use arrow::array::{BinaryArray, RecordBatch, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use clap::Args;
use itertools::izip;
use std::io::{stdin, stdout};
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct StellarAssetIdCommand {}

impl StellarAssetIdCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::UInt64,
            false,
        )]));

        loop {
            let reader = StreamReader::try_new_buffered(stdin(), None)?;
            let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

            for input_batch in reader {
                let input_batch = input_batch.context("failed to read input batch")?;
                let mut result_col_builder = UInt64Builder::with_capacity(input_batch.num_rows());
                let asset_code_col: &BinaryArray = input_batch.get_column("asset_code")?;
                let asset_issue_col: &BinaryArray = input_batch.get_column("asset_issuer")?;
                let asset_type_col: &BinaryArray = input_batch.get_column("asset_type")?;

                let it = izip!(asset_code_col, asset_issue_col, asset_type_col).map(
                    |(code, issuer, _type)| {
                        let buf: Vec<u8> =
                            [code.unwrap(), issuer.unwrap(), _type.unwrap()].concat();
                        farmhash::hash64(&buf)
                    },
                );

                for v in it {
                    result_col_builder.append_value(v);
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
