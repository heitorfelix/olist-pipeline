SELECT 
  order_id                      as idPedido	
, customer_id	                  as idCliente
, order_status	                as descStatusPedido
, order_purchase_timestamp	    as dtCompra
, order_approved_at	            as dtAprovacao
, order_delivered_carrier_date	as dtEnvio
, order_delivered_customer_date	as dtEntrega
, order_estimated_delivery_date as dtEntregaEstimada
 FROM bronze.olist.orders
