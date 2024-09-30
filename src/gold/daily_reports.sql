WITH pedidos_agg AS (
    SELECT
        DATE(o.dtCompra) AS dtRef,  -- Data de referência (data da compra)
        p.descCategoriaProduto AS descCategoriaProduto,  -- Categoria do produto
        COUNT(DISTINCT o.idPedido) AS nrQuantidadePedidos,  -- Quantidade de pedidos únicos
        COUNT(DISTINCT o.idCliente) AS nrQuantidadeClientes,  -- Quantidade de clientes distintos
        SUM(oi.vlrPreco) AS vlrTotalVendas,  -- Valor total de vendas
        SUM(oi.vlrFrete) AS vlrTotalFrete,  -- Valor total do frete
        SUM(CASE WHEN o.descStatusPedido = 'delivered' THEN oi.idPedido ELSE 0 END) AS vlrTotalVendasEntregues,  -- Valor de pedidos entregues
        COUNT(CASE WHEN o.descStatusPedido = 'delivered' THEN o.idPedido END) AS nrPedidosEntregues,  -- Quantidade de pedidos entregues
        COUNT(CASE WHEN o.descStatusPedido = 'canceled' THEN o.idPedido END) AS nrPedidosCancelados  -- Quantidade de pedidos cancelados
    FROM
        silver.olist.pedidos o
    LEFT JOIN 
        silver.olist.itens_pedido oi ON o.idPedido = oi.idPedido
    LEFT JOIN
        silver.olist.produtos p ON oi.idProduto = p.idProduto
    WHERE 
        DATE(o.dtCompra) = '{dt_ref}'  -- Data de referência fornecida
    GROUP BY
        dtRef, p.descCategoriaProduto
    GROUPING SETS (
        (dtRef, p.descCategoriaProduto),  -- Agrupamento por data e categoria do produto
        (dtRef)  -- Agrupamento apenas por data
    )
)
SELECT
    dtRef,
    descCategoriaProduto,
    nrQuantidadePedidos,
    nrQuantidadeClientes,
    vlrTotalVendas,
    vlrTotalFrete,
    vlrTotalVendasEntregues,
    nrPedidosEntregues,
    nrPedidosCancelados
FROM pedidos_agg
ORDER BY dtRef;
