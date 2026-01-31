use crate::stellar::ID;
use anyhow::{Context, Result};
use arrow::array::{Int32Array, Int64Builder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use clap::Args;
use itertools::izip;
use std::io::{stdin, stdout};
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct StellarIdCommand {}

impl StellarIdCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Int64,
            false,
        )]));

        loop {
            let reader = StreamReader::try_new(stdin(), None).context("failed to open reader")?;
            let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

            for input_batch in reader {
                let input_batch = input_batch.context("failed to read input batch")?;
                let mut result_col_builder = Int64Builder::with_capacity(input_batch.num_rows());
                let ledger_sequence_col: &Int32Array = input_batch.get_column("ledger_sequence")?;
                let transaction_order_col: &Int32Array =
                    input_batch.get_column("transaction_order")?;
                let operation_order_col: &Int32Array = input_batch.get_column("operation_order")?;

                let it = izip!(
                    ledger_sequence_col,
                    transaction_order_col,
                    operation_order_col
                )
                .map(|(l, t, o)| -> i64 {
                    ID {
                        ledger_sequence: l.unwrap(),
                        transaction_order: t.unwrap(),
                        operation_order: o.unwrap(),
                    }
                    .to_i64()
                });

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
