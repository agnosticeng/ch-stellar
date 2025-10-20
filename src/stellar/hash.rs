use super::galexie_ledgers::{DEFAULT_XDR_RW_DEPTH_LIMIT, DEFAULT_XDR_RW_LEN_LIMIT};
use super::result::{Result, StellarError};
use sha2::{Digest, Sha256};
use stellar_xdr::next::{
    FeeBumpTransaction, Hash, Limits, MuxedAccount, Preconditions, Transaction,
    TransactionEnvelope, TransactionExt, TransactionSignaturePayload,
    TransactionSignaturePayloadTaggedTransaction, TransactionV0, WriteXdr,
};

pub fn hash_transaction_envelope(
    envelope: TransactionEnvelope,
    passphrase: &str,
) -> Result<[u8; 32]> {
    match envelope {
        TransactionEnvelope::TxV0(tx) => hash_transaction_v0(tx.tx, passphrase),
        TransactionEnvelope::Tx(tx) => hash_transaction(tx.tx, passphrase),
        TransactionEnvelope::TxFeeBump(tx) => hash_fee_bump_transaction(tx.tx, passphrase),
    }
}

pub fn hash_transaction_v0(tx: TransactionV0, passphrase: &str) -> Result<[u8; 32]> {
    let source_account = MuxedAccount::Ed25519(tx.source_account_ed25519);

    let mut v1tx = Transaction {
        source_account,
        fee: tx.fee,
        memo: tx.memo,
        operations: tx.operations,
        seq_num: tx.seq_num,
        cond: Preconditions::None,
        ext: TransactionExt::V0,
    };

    if let Some(time_bounds) = tx.time_bounds {
        v1tx.cond = Preconditions::Time(time_bounds);
    }

    hash_transaction(v1tx, passphrase)
}

pub fn hash_transaction(tx: Transaction, passphrase: &str) -> Result<[u8; 32]> {
    hash_transaction_signature_payload_tagged_transaction(
        TransactionSignaturePayloadTaggedTransaction::Tx(tx),
        passphrase,
    )
}

pub fn hash_fee_bump_transaction(tx: FeeBumpTransaction, passphrase: &str) -> Result<[u8; 32]> {
    hash_transaction_signature_payload_tagged_transaction(
        TransactionSignaturePayloadTaggedTransaction::TxFeeBump(tx),
        passphrase,
    )
}

pub fn hash_transaction_signature_payload_tagged_transaction(
    tx: TransactionSignaturePayloadTaggedTransaction,
    passphrase: &str,
) -> Result<[u8; 32]> {
    if passphrase.trim().is_empty() {
        return Err(StellarError::EmptyNetworkPassphrase);
    }

    let network_id: [u8; 32] = Sha256::digest(passphrase).into();

    let payload = TransactionSignaturePayload {
        network_id: Hash(network_id),
        tagged_transaction: tx,
    };

    let tx_bytes = payload.to_xdr(Limits {
        depth: DEFAULT_XDR_RW_DEPTH_LIMIT,
        len: DEFAULT_XDR_RW_LEN_LIMIT,
    })?;

    Ok(Sha256::digest(tx_bytes).into())
}
