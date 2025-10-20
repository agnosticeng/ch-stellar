with 
    t as (
        select * from executable(
            'ch-stellar table-function stellar-rpc',
            ArrowStream,
            'result JSON',
            (
                with 
                    (
                        select 
                            stellar_rpc(
                                'https://rpc.lightsail.network/',
                                'getHealth', 
                                'null'
                            )
                    ) as health,

                    t as (
                        select 
                            'https://rpc.lightsail.network/' as endpoint,
                            'getLedgers' as method,
                            toJSONString(map(
                                'startLedger', (health.value.latestLedger-3)::UInt32,
                                'endLedger', (health.value.latestLedger-1)::UInt32
                            )) as params
                    )

                select * from t
            ),
            settings stderr_reaction='log', check_exit_code=true
        )
    )

select
    *
from t

settings output_format_arrow_string_as_string=0