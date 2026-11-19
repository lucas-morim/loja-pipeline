"""
===========================================================================
EXERCICIO: o teu ETL da estrela, agora em PySpark
===========================================================================

Tu ja escreveste este ETL. Em SQL, no estrela.sql. As regras de negocio
sao as mesmas, o modelo e o mesmo, os numeros certos sao os mesmos.
O que muda e a ferramenta -- e e esse o ponto do exercicio.

    python transformar.py        corre o teu ETL
    python verificar_spark.py    corrige

---------------------------------------------------------------------------
ENTRADA
---------------------------------------------------------------------------

Os cinco CSV em dados/ -- as mesmas cinco tabelas da loja.

Boa noticia: o leitor de CSV do Spark faz melhor do que o pandas naquilo
que te mordeu. Com inferSchema=True ele adivinha os tipos (int, double,
timestamp, date) e transforma campos vazios em NULL, nao em "". Ou seja,
o COALESCE funciona como esperas.

---------------------------------------------------------------------------
SAIDA
---------------------------------------------------------------------------

Cinco pastas de Parquet em saida/ -- a tua estrela:

    saida/dim_data/
    saida/dim_produto/
    saida/dim_cliente/
    saida/dim_pagamento/
    saida/facto_vendas/ano=2026/mes=1/   <- particionado

O main() ja esta escrito e trata de escrever. Tu escreves as cinco
funcoes construir_*.

---------------------------------------------------------------------------
PARTICIONAMENTO -- porque e que a facto_vendas leva ano= e mes=
---------------------------------------------------------------------------

O main() escreve a tabela de factos com .partitionBy("ano", "mes"), o que
cria uma pasta por combinacao. Isso significa que a tua construir_facto_vendas
TEM de devolver colunas chamadas ano e mes, ou a escrita falha.

Porque se faz isto: quando o Athena correr

    SELECT ... WHERE ano = 2026 AND mes = 3

ele olha para os NOMES DAS PASTAS e le so a de marco. Sem particoes, lia
os ficheiros todos para depois deitar fora 11/12 das linhas. Como o Athena
cobra por byte lido, particionar bem e literalmente a diferenca entre
cents e euros numa tabela grande.

As colunas ano e mes nao ficam dentro dos ficheiros Parquet -- ficam nos
nomes das pastas, e o leitor reconstroi-as. Nao ocupam espaco nenhum.

---------------------------------------------------------------------------
REGRAS DE NEGOCIO -- as mesmas de sempre
---------------------------------------------------------------------------

R1. receita = qtd * preco_unitario * (1 - desconto)
    o preco de itens_pedido, nao o preco_atual de produtos

R2. so pedidos com estado 'concluido' entram nos factos

R3. nao arredondes nada. A tabela de factos guarda o valor cru.

---------------------------------------------------------------------------
REGRAS DO MODELO -- tambem as mesmas
---------------------------------------------------------------------------

M1. Sem colunas de texto na facto_vendas. Nenhuma.

M2. Sem chaves NULL na facto_vendas. Os pedidos de convidado
    (cliente_id vazio na origem) apontam para o membro desconhecido.

M3. Uma linha por entidade em cada dimensao.

M4. Valores em falta viram texto, nao NULL:
      produto sem categoria        -> categoria = 'desconhecida'
      cliente sem morada           -> cidade = 'desconhecida'
                                      distrito = 'desconhecido'
      linha de convidado           -> cliente_id = -1
                                      nome = 'sem cliente'
                                      cidade = 'sem registo'
                                      distrito = 'sem registo'

M5. dim_data: data_id e um INTEGER AAAAMMDD (ex: 20260304)

M6. dia_semana: 1 = segunda ... 7 = domingo
    (atencao: o F.dayofweek() do Spark devolve 1 = domingo)

---------------------------------------------------------------------------
UM CENTIMO QUE TE VAI APARECER
---------------------------------------------------------------------------

Em fevereiro, a soma da receita da exactamente 11424.275 -- em cima do
meio centimo. E ai os motores discordam:

    SQLite  ROUND(11424.275, 2)  ->  11424.27
    Spark   round(11424.275, 2)  ->  11424.28

Nao e bug de nenhum dos dois. O float mais proximo de 11424.275 e
11424.2749999999996..., ligeiramente abaixo; o SQLite arredonda esse valor
e desce, o Spark trata o empate como empate e sobe.

Isto acontece a serio e da discussoes entre equipas que nao conseguem
reconciliar relatorios. A solucao profissional e nao guardar dinheiro em
float -- usa-se um tipo decimal, que e exacto. Nao vamos fazer isso aqui
para nao alargar o exercicio, mas fica a saber que e essa a resposta certa.

O verificador compara somas sem arredondar, por isso nao te vai chatear
com isto -- aceita um centimo de folga e avisa-te se aparecer. Mas quando
chegares ao Athena e comparares relatorios arredondados com os numeros que
tens do SQLite, e aqui que vais vir buscar a explicacao.

---------------------------------------------------------------------------
PISTAS DE API -- o que vais precisar e ainda nao usaste
---------------------------------------------------------------------------

    F.coalesce(col, F.lit("desconhecida"))    o COALESCE do SQL
    F.trim(col)                                limpar espacos
    F.to_date(col)                             timestamp -> date
    F.date_format(col, "yyyyMMdd")             formatar datas (devolve string)
    F.year(col), F.month(col)                  partes da data
    F.dayofmonth(col), F.dayofweek(col)
    df.distinct()                              SELECT DISTINCT
    df.unionByName(outro)                      juntar linhas (mesmas colunas)
    Window.orderBy("x") + F.row_number()       numerar linhas -> chave substituta
    spark.createDataFrame([(...)], schema=...) criar linhas a mao

Para juntar tabelas com colunas do mesmo nome, da-lhes alias e qualifica:

    a.alias("x").join(b.alias("y"), F.col("x.id") == F.col("y.id"), "left")
     .select(F.col("x.nome"), F.col("y.nome").alias("outro_nome"))

===========================================================================
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

AQUI = Path(__file__).resolve().parent


def obter_caminhos():
    """De onde ler e para onde escrever.

    Localmente: as pastas do projecto.
    No Glue:    o que vier nos argumentos --entrada e --saida.

    O parse_known_args ignora os argumentos que o Glue acrescenta sozinho
    (--JOB_NAME e companhia) em vez de rebentar com eles.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--entrada", default=str(AQUI.parent / "dados"))
    p.add_argument("--saida", default=str(AQUI.parent / "saida"))
    args, _ = p.parse_known_args()
    return args.entrada, args.saida


# ---------------------------------------------------------------------------
# dado: a sessao e a leitura
# ---------------------------------------------------------------------------

def criar_sessao():
    # Sem .master() de proposito: localmente o Spark assume local[*], e no
    # Glue pega no cluster que o ambiente ja definiu. Fixar o master aqui
    # faria o job correr numa so maquina sem dar erro nenhum.
    spark = (SparkSession.builder
             .appName("loja-etl")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def ler(spark, entrada, nome):
    """Le <entrada>/<nome>.csv com tipos inferidos e vazios como NULL."""
    return spark.read.csv(f"{entrada}/{nome}.csv", header=True, inferSchema=True)


# ---------------------------------------------------------------------------
# a partir daqui e teu
# ---------------------------------------------------------------------------

def construir_dim_data(pedidos):
    """Uma linha por dia.

    colunas obrigatorias:
        data_id     int      AAAAMMDD
        ano         int
        mes         int
        ano_mes     string   '2026-02'
        dia_semana  int      1=segunda .. 7=domingo

    Podes acrescentar as que quiseres (data, dia, nome_do_mes...).
    """
    
    return (pedidos.select(F.to_date(F.col("data_hora")).alias("data_hora")).distinct()
                    .select(
                        F.date_format(F.col("data_hora"), "yyyyMMdd").alias("data_id").cast("int"),
                        F.year(F.col("data_hora")).alias("ano"),
                        F.month(F.col("data_hora")).alias("mes"),
                        F.date_format(F.col("data_hora"), "yyyy-MM").alias("ano_mes"),
                        (((F.dayofweek(F.col("data_hora")) + 5) % 7) + 1).alias("dia_semana"),
                    ))

def construir_dim_produto(produtos, categorias):
    """Uma linha por produto.

    colunas obrigatorias:
        produto_id   int
        nome         string
        categoria    string    'desconhecida' quando nao ha
        preco_atual  double
    """

    produto_categoria = produtos.join(
           categorias.withColumnRenamed("nome", "categoria"),
           on = "categoria_id",
           how = "left"
    ).drop("categoria_id")

    return produto_categoria.select(F.col("produto_id"), F.col("nome"), F.coalesce(F.col("categoria"), F.lit("desconhecida")).alias("categoria"), F.col("preco_atual"))

def construir_dim_cliente(clientes):
    """Uma linha por cliente, mais a linha do convidado.

    colunas obrigatorias:
        cliente_id  int
        nome        string
        cidade      string    
        distrito    string
    """

    clientes_tratado = clientes.select(
        F.col("cliente_id"),
        F.col("nome"),
        F.coalesce(F.col("cidade"), F.lit("desconhecida")).alias("cidade"),
        F.coalesce(F.col("distrito"), F.lit("desconhecido")).alias("distrito")
    )

    clientes_df = clientes_tratado.sparkSession.createDataFrame(
        [(-1, "sem cliente", "sem registo", "sem registo")],
        schema = clientes_tratado.schema
    )

    return clientes_df.unionByName(clientes_tratado)

def construir_dim_pagamento(pedidos):
    """Uma linha por metodo de pagamento distinto.

    colunas obrigatorias:
        pagamento_id  int     chave que tu inventas
        metodo        string
    """
    janela = Window.orderBy("metodo")

    metodos = pedidos.select(F.trim(F.col("metodo_pagamento")).alias("metodo")).distinct()

    return metodos.withColumn("pagamento_id", F.row_number().over(janela))


def construir_facto_vendas(pedidos, itens, dim_pagamento):
    """Uma linha por produto dentro de um pedido concluido.

    colunas obrigatorias:
        data_id         int
        produto_id      int
        cliente_id      int
        pagamento_id    int
        pedido_id       int
        qtd             int
        receita         double
        ano             int      <- para o particionamento
        mes             int      <- idem

    Guardar tambem o preco_unitario e boa pratica; fica ao teu criterio.
    """
    pedidos_confirmados = pedidos.filter(F.col("estado") == "concluido")

    vendas = itens.join(pedidos_confirmados, on = "pedido_id", how = "inner").drop("item_id", "estado")
    vendas = vendas.join(dim_pagamento, F.trim(vendas["metodo_pagamento"]) == dim_pagamento["metodo"], how="left").drop("metodo", "metodo_pagamento")

    return vendas.withColumns({
        "cliente_id": F.coalesce(F.col("cliente_id"), F.lit(-1)),
        "data_id": F.date_format(F.col("data_hora"), "yyyyMMdd").cast("int"),
        "receita": (F.col("qtd") * F.col("preco_unitario") * (1 - F.col("desconto"))),
        "ano": F.year(F.col("data_hora")),
        "mes": F.month(F.col("data_hora"))
    }).drop("desconto", "data_hora")


# ---------------------------------------------------------------------------
# dado: a escrita
# ---------------------------------------------------------------------------

def main():
    entrada, saida = obter_caminhos()
    print(f"  entrada: {entrada}")
    print(f"  saida:   {saida}")

    spark = criar_sessao()

    categorias = ler(spark, entrada, "categorias")
    produtos = ler(spark, entrada, "produtos")
    clientes = ler(spark, entrada, "clientes")
    pedidos = ler(spark, entrada, "pedidos")
    itens = ler(spark, entrada, "itens_pedido")

    tabelas = {
        "dim_data": construir_dim_data(pedidos),
        "dim_produto": construir_dim_produto(produtos, categorias),
        "dim_cliente": construir_dim_cliente(clientes),
        "dim_pagamento": construir_dim_pagamento(pedidos),
    }
    facto = construir_facto_vendas(pedidos, itens, tabelas["dim_pagamento"])

    for nome, df in tabelas.items():
        # coalesce(1): as dimensoes sao pequenas e nao ha razao para as
        # espalhar por varios ficheiros. Muitos ficheiros minusculos tornam
        # qualquer motor de consulta lento, porque o custo e por ficheiro
        # aberto e nao por byte lido.
        df.coalesce(1).write.mode("overwrite").parquet(f"{saida}/{nome}")
        print(f"  {nome:<16} {df.count():>5} linhas")

    (facto.write.mode("overwrite")
          .partitionBy("ano", "mes")
          .parquet(f"{saida}/facto_vendas"))
    print(f"  {'facto_vendas':<16} {facto.count():>5} linhas")

    spark.stop()


if __name__ == "__main__":
    main()