SELECT
  order_id	            as idPedido
, order_item_id		      as nrItemPedido
, product_id			      as idProduto
, seller_id			        as idVendedor
, shipping_limit_date	  as dtEntregaLimite
, price				          as vlrPreco
, freight_value		      as vlrFrete
 FROM bronze.olist.order_items