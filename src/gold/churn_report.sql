WITH tb_new AS (
  SELECT DISTINCT 
      DATE('{dt_ref}') AS dtRef,
      t1.idCliente
  FROM silver.olist.pedidos AS t1
WHERE DATE(t1.dtCompra) BETWEEN '{dt_ref}'  - INTERVAL 28 DAY AND '{dt_ref}'
),

tb_old AS (
  SELECT DISTINCT
    DATE('{dt_ref}' - INTERVAL 28 DAY) AS dtRef,
    t1.idCliente
  FROM silver.olist.pedidos AS t1
WHERE DATE(t1.dtCompra) BETWEEN '{dt_ref}'  - INTERVAL 56 DAY AND '{dt_ref}' - INTERVAL 28 DAY
)
SELECT 
  DATE('dt_ref') AS dtRef,
  COUNT(t1.idCliente) AS qtdBaseOld,
  COUNT(t2.idCliente) AS qtdBaseNewNotChurn,
  count(t1.idCliente) - count(t2.idCliente)  AS nrQtdeChurn,
  1 - COUNT(t2.idCliente) / COUNT(t1.idCliente) AS ChurnRate
FROM tb_old as t1
LEFT JOIN tb_new as t2
ON t1.idCliente = t2.IdCliente
GROUP BY ALL