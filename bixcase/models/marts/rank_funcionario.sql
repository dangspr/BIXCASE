SELECT * FROM f.{{ ref('stg_funcionario') }},
    RANK() OVER (ORDER BY SUM(v.VENDA) DESC) AS RANK_VENDA,
    SUM(v.VENDA) AS VENDA_TOTAL
FROM {{ ref('stg_venda') }} v
INNER JOIN {{ ref('stg_funcionario') }} f ON v.ID_FUNCIONARIO = f.ID_FUNCIONARIO
GROUP BY f.ID_FUNCIONARIO, f.NOME_FUNCIONARIO
ORDER BY SUM(v.VENDA) DESC