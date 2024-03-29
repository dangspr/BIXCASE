SELECT 
    RANK() OVER (ORDER BY SUM(v.VENDA) DESC) AS RANK_VENDA, NOME_FUNCIONARIO,
    CAST(SUM(v.VENDA) AS FLOAT) AS VENDA_TOTAL
FROM {{ ref('stg_funcionario') }} f
INNER JOIN {{ ref('stg_venda') }} v ON f.ID_FUNCIONARIO = v.ID_FUNCIONARIO
GROUP BY f.ID_FUNCIONARIO, f.NOME_FUNCIONARIO
