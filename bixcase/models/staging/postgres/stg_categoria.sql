with raw_categoria as (
    select
        ID
        ,NOME_CATEGORIA
    from {{ source('postgres_data', 'categoria') }}
)

select * from raw_categoria