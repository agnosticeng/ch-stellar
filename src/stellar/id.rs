const LEDGER_MASK: i64 = (1 << 32) - 1;
const TRANSACTION_MASK: i64 = (1 << 20) - 1;
const OPERATION_MASK: i64 = (1 << 12) - 1;

const LEDGER_SHIFT: i64 = 32;
const TRANSACTION_SHIFT: i64 = 12;
const OPERATION_SHIFT: i64 = 0;

pub struct ID {
    pub ledger_sequence: i32,
    pub transaction_order: i32,
    pub operation_order: i32,
}

impl ID {
    pub fn to_i64(self) -> i64 {
        let mut i: i64 = 0;
        i |= (self.ledger_sequence as i64 & LEDGER_MASK) << LEDGER_SHIFT;
        i |= (self.transaction_order as i64 & TRANSACTION_MASK) << TRANSACTION_SHIFT;
        i |= (self.operation_order as i64 & OPERATION_MASK) << OPERATION_SHIFT;
        i
    }

    pub fn from_i64(i: i64) -> ID {
        ID {
            ledger_sequence: i32::try_from((i >> LEDGER_SHIFT) & LEDGER_MASK).unwrap(),
            transaction_order: i32::try_from((i >> TRANSACTION_SHIFT) & TRANSACTION_MASK).unwrap(),
            operation_order: i32::try_from((i >> OPERATION_SHIFT) & OPERATION_MASK).unwrap(),
        }
    }
}
