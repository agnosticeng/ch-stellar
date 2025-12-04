use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use ch_udf_common::json_result::JSONResult;
use ch_udf_common::json_rpc::{JSONRPCCall, JSONRpcClient};
use clap::Args;
use core::str;
use itertools::izip;
use serde_json::json;
use std::io::{stdin, stdout};
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct StellarRpcCommand {}

impl StellarRpcCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Binary,
            false,
        )]));
        let reader = StreamReader::try_new_buffered(stdin(), None)?;
        let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

        for input_batch in reader {
            let input_batch = input_batch.context("failed to read input batch")?;

            let endpoint_col: &BinaryArray = input_batch.get_column("endpoint")?;
            let method_col: &BinaryArray = input_batch.get_column("method")?;
            let params_col: &BinaryArray = input_batch.get_column("params")?;

            for (endpoint, method, params) in izip!(endpoint_col, method_col, params_col) {
                let mut curr = None;
                let client = JSONRpcClient::new(str::from_utf8(endpoint.unwrap())?)?;

                loop {
                    let mut params = serde_json::from_slice::<serde_json::Value>(params.unwrap())?;

                    if let Some(v) = curr {
                        if let Some(m) = params.as_object_mut() {
                            m.insert(
                                "pagination".to_string(),
                                json!({
                                    "cursor": v
                                }),
                            );
                            m.remove("startLedger");
                            m.remove("endLedger");
                        }
                    }

                    let mut res = client
                        .calls([JSONRPCCall {
                            method: str::from_utf8(method.unwrap())?.to_string(),
                            params,
                        }])
                        .await?;

                    let res = JSONResult::from(res.remove(0));
                    let mut result_col_builder: GenericByteBuilder<
                        arrow::datatypes::GenericBinaryType<i32>,
                    > = GenericByteBuilder::<BinaryType>::new();
                    result_col_builder.append_value(serde_json::to_string(&res)?.as_bytes());
                    let result_col = result_col_builder.finish();
                    let output_batch =
                        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result_col)])?;
                    writer
                        .write(&output_batch)
                        .context("failed to write output batch")?;
                    writer.flush().context("failed to flush output stream")?;

                    let res = Into::<Result<serde_json::Value>>::into(res);

                    let cursor = res
                        .as_ref()
                        .ok()
                        .as_ref()
                        .and_then(|x| x.as_object())
                        .and_then(|x| x.get("cursor"))
                        .and_then(|x| x.as_str());

                    match cursor {
                        None => break,
                        Some(v) => curr = Some(v.to_string()),
                    }
                }
            }
        }

        Ok(())
    }
}
