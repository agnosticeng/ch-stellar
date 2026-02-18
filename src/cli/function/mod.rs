mod stellar_asset_id;
mod stellar_galexie_tip;
mod stellar_hash_transaction;
mod stellar_id;
mod stellar_rpc;
mod stellar_strkey_decode;
mod stellar_uint256_to_account;
mod stellar_unmux;
mod stellar_xdr_decode;

use anyhow::Result;
use clap::{Args, Subcommand};
use stellar_asset_id::StellarAssetIdCommand;
use stellar_galexie_tip::StellarGalexieTipCommand;
use stellar_hash_transaction::StellarHashTransactionCommand;
use stellar_id::StellarIdCommand;
use stellar_rpc::StellarRpcCommand;
use stellar_strkey_decode::StellarStrkeyDecodeCommand;
use stellar_uint256_to_account::StellarUint256ToAccountCommand;
use stellar_unmux::StellarUnmuxCommand;
use stellar_xdr_decode::StellarXdrDecodeCommand;

#[derive(Debug, Clone, Subcommand)]
pub enum FunctionCommand {
    Rpc(StellarRpcCommand),
    XdrDecode(StellarXdrDecodeCommand),
    HashTransaction(StellarHashTransactionCommand),
    Id(StellarIdCommand),
    StrkeyDecode(StellarStrkeyDecodeCommand),
    Unmux(StellarUnmuxCommand),
    AssetId(StellarAssetIdCommand),
    GalexieTip(StellarGalexieTipCommand),
    Uint256ToAccount(StellarUint256ToAccountCommand),
}

#[derive(Clone, Debug, Args)]
pub struct Function {
    #[command(subcommand)]
    pub cmd: FunctionCommand,
}

impl Function {
    pub async fn run(&self) -> Result<()> {
        match &self.cmd {
            FunctionCommand::Rpc(cmd) => cmd.run().await,
            FunctionCommand::XdrDecode(cmd) => cmd.run().await,
            FunctionCommand::HashTransaction(cmd) => cmd.run().await,
            FunctionCommand::Id(cmd) => cmd.run().await,
            FunctionCommand::StrkeyDecode(cmd) => cmd.run().await,
            FunctionCommand::Unmux(cmd) => cmd.run().await,
            FunctionCommand::AssetId(cmd) => cmd.run().await,
            FunctionCommand::GalexieTip(cmd) => cmd.run().await,
            FunctionCommand::Uint256ToAccount(cmd) => cmd.run().await,
        }
    }
}
