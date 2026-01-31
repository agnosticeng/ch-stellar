with
    t as (
        select
            *
        from generateRandom('
            ledger_sequence Int32,
            transaction_order Int32,
            operation_order Int32
        ')
        limit 1000000000
    )

select
    *,
    stellar_id(ledger_sequence, transaction_order, operation_order) as stellar_id
from t
