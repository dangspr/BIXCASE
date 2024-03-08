with raw_funcionario as (
    select
        ID_FUNCIONARIO
        ,NOME_FUNCIONARIO
    from {{ source('api_data', 'funcionario') }}
)

select * from raw_funcionario