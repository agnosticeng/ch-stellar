use crate::stellar::galexie_ledgers;
use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch, UInt32Array};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use ch_udf_common::io::{open_input, open_output};
use clap::Args;
use futures::{StreamExt, pin_mut};
use futures::{TryStreamExt, stream};
use itertools::izip;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct StellarGalexieCommand {
    #[arg(long)]
    input_file: Option<String>,
    #[arg(long)]
    output_file: Option<String>,
    #[arg(long)]
    output_block_size: Option<usize>,
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

        let s = stream::iter(reader)
            .err_into::<anyhow::Error>()
            .and_then(|input_batch| async move {
                let url_col: &BinaryArray = input_batch.get_column("url")?;
                let start_col: &UInt32Array = input_batch.get_column("start")?;
                let end_col: &UInt32Array = input_batch.get_column("end")?;

                let data = izip!(url_col, start_col, end_col)
                    .map(|(url, start, end)| {
                        Ok::<_, anyhow::Error>((
                            url.ok_or(anyhow::anyhow!("no URL"))
                                .and_then(|x| Ok(String::from_utf8(x.to_vec())?))?,
                            start,
                            end,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let s = stream::iter(data).map(move |(url, start, end)| {
                    Ok::<_, anyhow::Error>(
                        galexie_ledgers(&url, start, end)?.err_into::<anyhow::Error>(),
                    )
                });
                Ok(s)
            })
            .try_flatten()
            .try_flatten()
            .chunks(self.output_block_size.unwrap_or(32))
            .map(
                |batch| -> Result<arrow::array::RecordBatch, anyhow::Error> {
                    let mut result_col_builder: GenericByteBuilder<
                        arrow::datatypes::GenericBinaryType<i32>,
                    > = GenericByteBuilder::<BinaryType>::new();

                    for lcm in batch {
                        result_col_builder.append_value(serde_json::to_vec(&lcm?)?);
                    }

                    let result_col = result_col_builder.finish();

                    Ok(RecordBatch::try_new(
                        output_schema.clone(),
                        vec![Arc::new(result_col)],
                    )?)
                },
            );

        pin_mut!(s);

        while let Some(batch) = s.next().await {
            writer
                .write(&batch?)
                .context("failed to write output batch")?;

            writer.flush().context("failed to flush output stream")?;
        }

        Ok(())
    }
}
