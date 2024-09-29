SELECT 
  product_id                           AS idProduto,
  product_category_name                AS descCategoriaProduto,
  product_name_length                  AS comprimentoNomeProduto,
  product_description_length           AS comprimentoDescricaoProduto,
  product_photos_qty                   AS quantidadeFotosProduto,
  product_weight_g                     AS pesoProduto,
  product_length_cm                    AS comprimentoProduto,
  product_height_cm                    AS alturaProduto,
  product_width_cm                     AS larguraProduto
FROM bronze.olist.products
