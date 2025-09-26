use core::str;
use std::io::{stdin,stdout};
use std::sync::Arc;
use anyhow::{Context, Result};
use clap::Args;
use itertools::izip;
use futures::{StreamExt,pin_mut};
use arrow::datatypes::{Schema,DataType,Field,BinaryType};
use arrow::array::{BinaryArray,GenericByteBuilder,RecordBatch,UInt32Array};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use crate::stellar::galexie_ledgers;
use crate::arrow_ext::RecordBatchExt;

#[derive(Debug, Clone, Args)]
pub struct StellarGalexieCommand {}

impl StellarGalexieCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new("ledger_close_meta", DataType::Binary, false)]));
        let reader = StreamReader::try_new_buffered(stdin(), None)?;
        let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

        for input_batch in reader {
            let input_batch = input_batch.context("failed to read input batch")?;

            let mut result_col_builder = GenericByteBuilder::<BinaryType>::new();

            let url_col: &BinaryArray = input_batch.get_column("url")?;
            let start_col: &UInt32Array = input_batch.get_column("start")?;
            let end_col: &UInt32Array = input_batch.get_column("end")?;

            for (url, start, end) in izip!(url_col, start_col, end_col) {
                let stream= galexie_ledgers(
                    str::from_utf8(url.unwrap_or_default())?,
                    start, 
                    end
                )?;

                pin_mut!(stream);

                while let Some(res) = stream.next().await {
                    result_col_builder.append_value(serde_json::to_string(&res?)?.as_bytes());
                }
            }

            let result_col = result_col_builder.finish();
            let output_batch = RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result_col)])?;
            writer.write(&output_batch).context("failed to write output batch")?;
            writer.flush().context("failed to flush output stream")?;
        }

        Ok(())
    }
}