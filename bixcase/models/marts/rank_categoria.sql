//select 'script' as fazer from {{ ref('stg_funcionario') }} , {{ ref('stg_venda') }}, {{ ref('stg_categoria') }}
SELECT
    c.NOME_CATEGORIA,
    RANK() OVER (ORDER BY SUM(v.VENDA) DESC) AS RANK_VENDA,
    SUM(v.VENDA) AS VENDA_TOTAL
FROM {{ ref('stg_venda') }} v
INNER JOIN {{ ref('stg_categoria') }} c ON v.ID_CATEGORIA = c.ID
GROUP BY c.ID, c.NOME_CATEGORIA
ORDER BY SUM(v.VENDA) DESC;