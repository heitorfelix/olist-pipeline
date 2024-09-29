SELECT 				      
order_id	                as idPedido
, payment_sequential	    as sequenciaPagamento
, payment_type	          as descTipoPagamento
, payment_installments	  as numParcelas
, payment_value           as vlrPagamento
 FROM bronze.olist.order_payments