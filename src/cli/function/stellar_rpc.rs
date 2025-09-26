use core::str;
use std::io::{stdin,stdout};
use std::sync::Arc;
use anyhow::{Context,Result,bail,anyhow};
use clap::Args;
use itertools::izip;
use arrow::datatypes::{Schema,DataType,Field,BinaryType};
use arrow::array::{BinaryArray,GenericByteBuilder,RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use crate::json_rpc::{JSONRpcClient,JSONRPCCall};
use crate::json_result::JSONResult;
use crate::arrow_ext::RecordBatchExt;

#[derive(Debug, Clone, Args)]
pub struct StellarRpcCommand {}

impl StellarRpcCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new("result", DataType::Binary, false)]));

        loop {
            let reader = StreamReader::try_new_buffered(stdin(), None)?;
            let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

            for input_batch in reader {
                let input_batch = input_batch.context("failed to read input batch")?;

                let mut result_col_builder = GenericByteBuilder::<BinaryType>::with_capacity(
                    input_batch.num_rows(),
                    input_batch.num_rows() * 1024
                );

                let method_col: &BinaryArray = input_batch.get_column("method")?;
                let endpoint_col: &BinaryArray = input_batch.get_column("endpoint")?;
                let params_col: &BinaryArray = input_batch.get_column("params")?; 

                if !endpoint_col.iter().all(|x| x.is_some() && x.unwrap() == endpoint_col.value(0)) {
                    bail!("endpoint must be constant for an input block");
                }

                let client = JSONRpcClient::new(str::from_utf8(endpoint_col.value(0))?)?;
                let mut calls = Vec::new();

                for (method, params) in izip!(method_col, params_col) {
                    calls.push(JSONRPCCall{
                        method: str::from_utf8(method.ok_or(anyhow!("method is not valid utf8"))?)?.to_string(),
                        params: serde_json::from_slice::<serde_json::Value>(params.unwrap()).context("failed to decode params")?

                    });
                }

                for res in client.calls(calls).await?.into_iter() {
                    result_col_builder.append_value(serde_json::to_string(&JSONResult::from(res))?.as_bytes());
                }

                let result_col = result_col_builder.finish();
                let output_batch = RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result_col)])?;
                writer.write(&output_batch).context("failed to write output batch")?;
                writer.flush().context("failed to flush output stream")?;
            }
        }
    }
}