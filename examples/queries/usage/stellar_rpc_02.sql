with 
    (
        select 
            stellar_rpc(
                'https://rpc.lightsail.network/',
                'getHealth', 
                'null'
            )
    ) as health
    
select 
    stellar_rpc(
        'https://rpc.lightsail.network/',
        'getTransactions',
        toJSONString(map(
            'startLedger', (health.value.latestLedger-1)::UInt32
        ))
    )

settings output_format_arrow_string_as_string=0
