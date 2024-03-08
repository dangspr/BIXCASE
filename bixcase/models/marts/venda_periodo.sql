select * from {{ ref('stg_venda') }}
  date_trunc(data_venda, 'MONTH') as periodo,
  sum(venda) over (order by date_trunc(data_venda, 'MONTH')) as venda_acumulada
from {{ ref('stg_venda') }}
where data_venda between "2015-01-01" and "2024-01-01"
order by 1;
