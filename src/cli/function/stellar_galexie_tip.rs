use crate::stellar::galexie_tip;
use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{BinaryArray, RecordBatch, UInt32Builder};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use ch_udf_common::json_result::JSONResult;
use ch_udf_common::json_rpc::{JSONRPCCall, JSONRpcClient};
use clap::Args;
use core::str;
use itertools::izip;
use std::io::{stdin, stdout};
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct StellarGalexieTipCommand {}

impl StellarGalexieTipCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::UInt32,
            false,
        )]));

        loop {
            let reader = StreamReader::try_new_buffered(stdin(), None)?;
            let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

            for input_batch in reader {
                let input_batch = input_batch.context("failed to read input batch")?;
                let mut result_col_builder = UInt32Builder::with_capacity(input_batch.num_rows());
                let url_col: &BinaryArray = input_batch.get_column("url")?;

                for url in url_col {
                    result_col_builder
                        .append_value(galexie_tip(str::from_utf8(url.unwrap())?).await?);
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
