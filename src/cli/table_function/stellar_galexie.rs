use crate::stellar::galexie_ledgers;
use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch, UInt32Array};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use ch_udf_common::io::{open_input, open_output};
use clap::Args;
use core::str;
use futures::{StreamExt, pin_mut};
use itertools::izip;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct StellarGalexieCommand {
    #[arg(long)]
    input_file: Option<String>,
    #[arg(long)]
    output_file: Option<String>,
}

impl StellarGalexieCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "ledger_close_meta",
            DataType::Binary,
            false,
        )]));
        let reader = StreamReader::try_new_buffered(
            BufReader::new(open_input(self.input_file.as_deref())?),
            None,
        )?;
        let mut writer = StreamWriter::try_new(
            BufWriter::new(open_output(self.output_file.as_deref())?),
            &output_schema,
        )?;

        for input_batch in reader {
            let input_batch = input_batch.context("failed to read input batch")?;

            let mut result_col_builder: GenericByteBuilder<
                arrow::datatypes::GenericBinaryType<i32>,
            > = GenericByteBuilder::<BinaryType>::new();

            let url_col: &BinaryArray = input_batch.get_column("url")?;
            let start_col: &UInt32Array = input_batch.get_column("start")?;
            let end_col: &UInt32Array = input_batch.get_column("end")?;

            for (url, start, end) in izip!(url_col, start_col, end_col) {
                let stream = galexie_ledgers(str::from_utf8(url.unwrap_or_default())?, start, end)?;
                pin_mut!(stream);

                while let Some(res) = stream.next().await {
                    let js = serde_json::to_vec(&res?)?;
                    result_col_builder.append_value(js);
                }
            }

            let result_col = result_col_builder.finish();

            let output_batch =
                RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result_col)])?;

            writer
                .write(&output_batch)
                .context("failed to write output batch")?;

            writer.flush().context("failed to flush output stream")?;
        }

        Ok(())
    }
}
