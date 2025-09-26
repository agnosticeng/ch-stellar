with 
    (
        select 
            stellar_rpc(
                'https://rpc.lightsail.network/',
                'getHealth', 
                'null'
            )
    ) as health,

     events as (
        select
            arrayJoin(res.value.events::Array(JSON)) as evt 
        from (
            select 
                stellar_rpc(
                    'https://rpc.lightsail.network/',
                    'getEvents',
                    toJSONString(map(
                        'startLedger', (health.value.latestLedger-10)::UInt32
                    ))
                ) as res
        )
    )
    
select
    evt.ledger::UInt32 as ledger,
    evt.contractId::String as contract_id,
    evt.type as type,
    arrayMap(x -> stellar_xdr_decode('ScVal', evt.value::String), evt.topic::Array(String)) as topics,
    stellar_xdr_decode('ScVal', evt.value::String) as value
from events

settings output_format_arrow_string_as_string=0