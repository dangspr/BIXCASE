with raw_venda as (
    select
        DATA_VENDA
        ,ID_CATEGORIA
        ,ID_FUNCIONARIO
        ,ID_VENDA
        ,VENDA
    from {{ source('postgres_data', 'venda') }}
)

select * from raw_venda