use crate::arrow_ext::RecordBatchExt;
use crate::json_result::JSONResult;
use crate::stellar::DEFAULT_XDR_RW_DEPTH_LIMIT;
use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use clap::Args;
use core::str;
use itertools::izip;
use std::io::{stdin, stdout};
use std::str::FromStr;
use std::sync::Arc;
use stellar_xdr::next::{Limited, Limits, Type, TypeVariant};

#[derive(Debug, Clone, Args)]
pub struct StellarXdrDecodeCommand {}

impl StellarXdrDecodeCommand {
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

                let type_col: &BinaryArray = input_batch.get_column("type")?;
                let data_col: &BinaryArray = input_batch.get_column("data")?;

                let it = izip!(type_col, data_col).map(|(_type, data)| -> Result<Type> {
                    let type_name =
                        str::from_utf8(_type.unwrap()).context("type lust be valid utf8")?;
                    let type_variant =
                        TypeVariant::from_str(type_name).context("failed to decode XDR data")?;
                    let mut xdr_reader =
                        Limited::new(data.unwrap(), Limits::depth(DEFAULT_XDR_RW_DEPTH_LIMIT));
                    let res = Type::read_xdr_base64_to_end(type_variant, &mut xdr_reader)?;
                    Ok(res)
                });

                for v in it {
                    result_col_builder.append_value(serde_json::to_string(&JSONResult::from(v))?);
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
