use stellar_xdr::curr::LedgerCloseMeta;

pub trait LedgerCloseMetaExt {
    fn ledger_seq(&self) -> u32;
}

impl LedgerCloseMetaExt for LedgerCloseMeta {
    fn ledger_seq(&self) -> u32 {
        match &self {
            LedgerCloseMeta::V0(lcm) => lcm.ledger_header.header.ledger_seq,
            LedgerCloseMeta::V1(lcm) => lcm.ledger_header.header.ledger_seq,
            LedgerCloseMeta::V2(lcm) => lcm.ledger_header.header.ledger_seq,
        }
    }
}
