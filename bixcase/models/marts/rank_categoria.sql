SELECT
    c.NOME_CATEGORIA,
    RANK() OVER (ORDER BY SUM(v.VENDA) DESC) AS RANK_VENDA,
    CAST(SUM(v.VENDA)AS FLOAT) AS VENDA_TOTAL
FROM {{ ref('stg_venda') }} v
INNER JOIN {{ ref('stg_categoria') }} c ON v.ID_CATEGORIA = c.ID
GROUP BY c.ID, c.NOME_CATEGORIA
