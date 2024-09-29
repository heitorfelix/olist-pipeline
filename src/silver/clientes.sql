SELECT 
  customer_id                          AS idCliente,
  customer_unique_id                   AS idClienteUnico,
  customer_zip_code_prefix              AS prefixCepCliente,
  customer_city                         AS descCidadeCliente,
  customer_state                        AS descEstadoCliente
FROM bronze.olist.customers
