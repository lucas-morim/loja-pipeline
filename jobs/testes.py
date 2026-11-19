from pyspark.sql import functions as F
from transformar import criar_sessao, ler, construir_dim_pagamento
from pyspark.sql import Window

spark = criar_sessao()
pedidos = ler(spark, "pedidos")
itens = ler(spark, "itens_pedido")
pagamento = construir_dim_pagamento(pedidos)

pedidos_confirmados = pedidos.filter(F.col("estado") == "concluido")

vendas = itens.join(pedidos_confirmados, on = "pedido_id", how = "inner").drop("item_id", "estado")
vendas = vendas.join(pagamento, on = "metodo_pagamento", how="inner").drop("metodo", "metodo_pagamento")

vendas = vendas.withColumns({
    "cliente_id": F.coalesce(F.col("cliente_id"), F.lit(-1)),
    "data_hora": F.date_format(F.col("data_hora"), "yyyyMMdd").cast("int"),
    "receita": (F.col("qtd") * F.col("preco_unitario") * (1 - F.col("desconto")))
}).drop("desconto").show()
