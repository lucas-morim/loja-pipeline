"""
Verificador do ETL em PySpark.

    python verificar_spark.py

Corre o teu transformar.py, abre o Parquet que ele escreveu, e compara.

As respostas esperadas NAO saem do teu Parquet nem do Spark. Sao calculadas
outra vez, em Python puro, lendo os CSV com o modulo csv. Dois caminhos
completamente diferentes: se discordarem, um dos dois esta errado.
"""

import csv
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

AQUI = Path(__file__).resolve().parent
DADOS = AQUI.parent / "dados"
SAIDA = AQUI.parent / "saida"
SCRIPT = AQUI / "transformar.py"

LARGURA = 48
CENTIMO = 0.011          # tolera um centimo (ver o cabecalho do transformar.py)


# ---------------------------------------------------------------------------
# as respostas certas, em Python puro, a partir dos CSV
# ---------------------------------------------------------------------------

def esperado():
    def tabela(nome):
        with open(DADOS / f"{nome}.csv", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    categorias = {r["categoria_id"]: r["nome"] for r in tabela("categorias")}
    produtos = {r["produto_id"]: r for r in tabela("produtos")}
    clientes = {r["cliente_id"]: r for r in tabela("clientes")}
    pedidos = {r["pedido_id"]: r for r in tabela("pedidos")}
    itens = tabela("itens_pedido")

    por_mes = defaultdict(float)
    por_categoria = defaultdict(float)
    por_distrito = defaultdict(float)
    por_dia_semana = defaultdict(float)
    por_metodo = defaultdict(float)
    total = 0.0
    linhas = 0
    pedidos_distintos = set()

    import datetime

    for i in itens:
        p = pedidos[i["pedido_id"]]
        if p["estado"] != "concluido":
            continue
        linhas += 1
        pedidos_distintos.add(i["pedido_id"])

        receita = (int(i["qtd"]) * float(i["preco_unitario"])
                   * (1 - float(i["desconto"])))
        total += receita

        d = datetime.datetime.strptime(p["data_hora"], "%Y-%m-%d %H:%M:%S")
        por_mes[d.strftime("%Y-%m")] += receita
        por_dia_semana[d.weekday() + 1] += receita      # Monday = 0 -> 1

        prod = produtos[i["produto_id"]]
        por_categoria[categorias.get(prod["categoria_id"], "desconhecida")] += receita

        if not p["cliente_id"]:
            por_distrito["sem registo"] += receita
        else:
            cl = clientes[p["cliente_id"]]
            por_distrito[cl["distrito"] or "desconhecido"] += receita

        por_metodo[p["metodo_pagamento"].strip()] += receita

    return {
        "linhas_facto": linhas,
        "pedidos_distintos": len(pedidos_distintos),
        "n_produtos": len(produtos),
        "n_clientes": len(clientes),
        "n_metodos": len({p["metodo_pagamento"].strip() for p in pedidos.values()}),
        "total": total,
        "por_mes": dict(por_mes),
        "por_categoria": dict(por_categoria),
        "por_distrito": dict(por_distrito),
        "por_dia_semana": dict(por_dia_semana),
        "por_metodo": dict(por_metodo),
    }


# ---------------------------------------------------------------------------
# apresentacao
# ---------------------------------------------------------------------------

class Relatorio:
    def __init__(self):
        self.falhas = 0
        self.notas = 0

    def ok(self, etiqueta, info=""):
        print(f"  {etiqueta:<{LARGURA}} OK" + (f"   {info}" if info else ""))

    def falhou(self, etiqueta, detalhe=""):
        self.falhas += 1
        print(f"  {etiqueta:<{LARGURA}} FALHOU")
        for linha in str(detalhe).split("\n"):
            if linha:
                print(f"      {linha}")

    def nota(self, etiqueta, detalhe=""):
        self.notas += 1
        print(f"  {etiqueta:<{LARGURA}} OK, com nota")
        for linha in str(detalhe).split("\n"):
            if linha:
                print(f"      {linha}")

    def checar(self, etiqueta, condicao, detalhe=""):
        if condicao:
            self.ok(etiqueta)
        else:
            self.falhou(etiqueta, detalhe)


def comparar_agregado(rel, titulo, obtido, esp):
    """Compara dois dicionarios chave -> valor monetario."""
    faltam = sorted(set(esp) - set(obtido))
    sobram = sorted(set(obtido) - set(esp))
    if faltam or sobram:
        detalhe = []
        if faltam:
            detalhe.append(f"faltam estas chaves: {faltam}")
        if sobram:
            detalhe.append(f"aparecem a mais: {sobram}")
        rel.falhou(titulo, "\n".join(detalhe))
        return

    erradas, centimos = [], []
    for chave in sorted(esp):
        dif = abs(obtido[chave] - esp[chave])
        if dif > CENTIMO:
            erradas.append(f"{chave:<16} obtido {obtido[chave]:>12.2f}"
                           f"   esperado {esp[chave]:>12.2f}"
                           f"   dif {obtido[chave] - esp[chave]:>+10.2f}")
        elif dif > 1e-6:
            centimos.append(f"{chave:<16} {obtido[chave]:.2f} vs {esp[chave]:.2f}")

    if erradas:
        rel.falhou(titulo, "\n".join(erradas))
    elif centimos:
        rel.nota(titulo, "diferenca de um centimo (arredondamento em empate):\n  "
                         + "\n  ".join(centimos))
    else:
        rel.ok(titulo)


# ---------------------------------------------------------------------------
# corrida
# ---------------------------------------------------------------------------

def correr_etl():
    if SAIDA.exists():
        shutil.rmtree(SAIDA)
    print("A correr o transformar.py...\n")
    r = subprocess.run([sys.executable, str(SCRIPT)], cwd=str(AQUI),
                       capture_output=True, text=True)
    saida = [l for l in (r.stdout + r.stderr).splitlines()
             if l.strip() and "WARN" not in l and "log4j" not in l
             and "Picked up" not in l and "FutureWarning" not in l
             and "require_minimum" not in l and not l.startswith("[Stage")]
    if r.returncode != 0:
        print("  O transformar.py falhou:\n")
        for l in saida[-25:]:
            print("   ", l)
        return False
    for l in saida:
        print("   ", l)
    return True


def main():
    if not SCRIPT.exists():
        print(f"nao encontrei {SCRIPT.name}")
        return 1

    if not correr_etl():
        return 1

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (SparkSession.builder.master("local[*]").appName("verificar")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    rel = Relatorio()
    esp = esperado()

    print()
    print("=" * 66)
    print("ESTRUTURA")
    print("=" * 66)

    nomes = ["dim_data", "dim_produto", "dim_cliente", "dim_pagamento", "facto_vendas"]
    dfs = {}
    for nome in nomes:
        caminho = SAIDA / nome
        if not caminho.exists():
            rel.falhou(f"{nome}: escrito", f"nao existe {caminho}")
            continue
        try:
            dfs[nome] = spark.read.parquet(str(caminho))
            dfs[nome].createOrReplaceTempView(nome)
        except Exception as e:
            rel.falhou(f"{nome}: legivel", str(e)[:300])

    if len(dfs) < 5:
        print("\n  Faltam tabelas; o resto das verificacoes depende delas.")
        spark.stop()
        return 1

    for nome in nomes:
        rel.ok(f"{nome}: escrito e legivel",
               f"{dfs[nome].count():>5} linhas, {len(dfs[nome].columns)} colunas")

    obrigatorias = {
        "dim_data": ["data_id", "ano", "mes", "ano_mes", "dia_semana"],
        "dim_produto": ["produto_id", "nome", "categoria", "preco_atual"],
        "dim_cliente": ["cliente_id", "nome", "cidade", "distrito"],
        "dim_pagamento": ["pagamento_id", "metodo"],
        "facto_vendas": ["data_id", "produto_id", "cliente_id", "pagamento_id",
                         "pedido_id", "qtd", "receita", "ano", "mes"],
    }
    for nome, cols in obrigatorias.items():
        tem = {c.lower() for c in dfs[nome].columns}
        faltam = [c for c in cols if c not in tem]
        rel.checar(f"{nome}: colunas obrigatorias", not faltam,
                   f"faltam: {', '.join(faltam)}")

    print()
    print("=" * 66)
    print("REGRAS DO MODELO")
    print("=" * 66)

    facto = dfs["facto_vendas"]

    # M1 -- sem texto nos factos (ano/mes sao particoes, nao contam como dados)
    texto = [c for c, t in facto.dtypes if t == "string"]
    rel.checar("facto_vendas sem colunas de texto", not texto,
               f"colunas de texto: {', '.join(texto)}")

    # M2 -- sem chaves NULL
    chaves = [c for c in facto.columns if c.endswith("_id")]
    com_null = [f"{c}: {facto.filter(F.col(c).isNull()).count()} linhas"
                for c in chaves if facto.filter(F.col(c).isNull()).count()]
    rel.checar("facto_vendas sem chaves NULL", not com_null, "\n".join(com_null))

    # grao
    rel.checar(f"facto_vendas: grao ({esp['linhas_facto']} linhas)",
               facto.count() == esp["linhas_facto"],
               f"obtido {facto.count()}, esperado {esp['linhas_facto']}")

    # M3 -- chave unica nas dimensoes
    nao_unicas = []
    for nome, chave in [("dim_data", "data_id"), ("dim_produto", "produto_id"),
                        ("dim_cliente", "cliente_id"), ("dim_pagamento", "pagamento_id")]:
        if chave not in [c.lower() for c in dfs[nome].columns]:
            continue
        t = dfs[nome].count()
        d = dfs[nome].select(chave).distinct().count()
        if t != d:
            nao_unicas.append(f"{nome}: {t} linhas, {d} {chave} distintos")
    rel.checar("dimensoes com chave unica", not nao_unicas, "\n".join(nao_unicas))

    # integridade
    orfas = []
    for chave, tabela in [("data_id", "dim_data"), ("produto_id", "dim_produto"),
                          ("cliente_id", "dim_cliente"), ("pagamento_id", "dim_pagamento")]:
        n = spark.sql(f"SELECT COUNT(*) n FROM facto_vendas f "
                      f"LEFT ANTI JOIN {tabela} d ON f.{chave} = d.{chave}").first()["n"]
        if n:
            orfas.append(f"{chave}: {n} linhas sem par em {tabela}")
    rel.checar("chaves dos factos resolvem nas dimensoes", not orfas, "\n".join(orfas))

    # M4 -- membros desconhecidos
    rel.checar("dim_produto: categoria 'desconhecida'",
               dfs["dim_produto"].filter(F.col("categoria") == "desconhecida").count() >= 1)
    rel.checar("dim_cliente: distrito 'desconhecido'",
               dfs["dim_cliente"].filter(F.col("distrito") == "desconhecido").count() >= 1)
    convidado = dfs["dim_cliente"].filter(
        (F.col("cliente_id") == -1) & (F.col("distrito") == "sem registo")).count()
    aponta = facto.filter(F.col("cliente_id") == -1).count()
    rel.checar("dim_cliente: linha de convidado (-1), e usada",
               convidado == 1 and aponta > 0,
               f"linhas com cliente_id=-1 na dimensao: {convidado}; "
               f"linhas de facto que la apontam: {aponta}")

    # M5 -- data_id no formato AAAAMMDD
    fora = dfs["dim_data"].filter(
        (F.col("data_id") < 20000101) | (F.col("data_id") > 20991231)).count()
    rel.checar("dim_data: data_id no formato AAAAMMDD", fora == 0,
               f"{fora} linhas fora do intervalo")

    # particionamento
    pastas = sorted(p.name for p in (SAIDA / "facto_vendas").iterdir() if p.is_dir())
    tem_ano = [p for p in pastas if p.startswith("ano=")]
    sub = []
    if tem_ano:
        sub = sorted(p.name for p in (SAIDA / "facto_vendas" / tem_ano[0]).iterdir()
                     if p.is_dir())
    rel.checar("facto_vendas particionada por ano e mes",
               bool(tem_ano) and any(p.startswith("mes=") for p in sub),
               f"pastas encontradas: {pastas[:5]}")

    print()
    print("=" * 66)
    print("OS NUMEROS   (calculados outra vez, em Python puro, a partir dos CSV)")
    print("=" * 66)

    def agregado(sql, chave_col, valor_col="r"):
        return {linha[chave_col]: linha[valor_col] for linha in spark.sql(sql).collect()}

    total = spark.sql("SELECT SUM(receita) r FROM facto_vendas").first()["r"]
    dif = abs(total - esp["total"])
    if dif > CENTIMO:
        rel.falhou("receita total",
                   f"obtido {total:.2f}, esperado {esp['total']:.2f}, "
                   f"dif {total - esp['total']:+.2f}")
    elif dif > 1e-6:
        rel.nota("receita total", f"{total:.2f} vs {esp['total']:.2f}")
    else:
        rel.ok("receita total", f"{total:.2f}")

    comparar_agregado(rel, "receita por mes", agregado(
        "SELECT d.ano_mes k, SUM(f.receita) r FROM facto_vendas f "
        "JOIN dim_data d ON f.data_id = d.data_id GROUP BY k", "k"), esp["por_mes"])

    comparar_agregado(rel, "receita por categoria", agregado(
        "SELECT p.categoria k, SUM(f.receita) r FROM facto_vendas f "
        "JOIN dim_produto p ON f.produto_id = p.produto_id GROUP BY k", "k"),
        esp["por_categoria"])

    comparar_agregado(rel, "receita por distrito", agregado(
        "SELECT c.distrito k, SUM(f.receita) r FROM facto_vendas f "
        "JOIN dim_cliente c ON f.cliente_id = c.cliente_id GROUP BY k", "k"),
        esp["por_distrito"])

    comparar_agregado(rel, "receita por metodo de pagamento", agregado(
        "SELECT g.metodo k, SUM(f.receita) r FROM facto_vendas f "
        "JOIN dim_pagamento g ON f.pagamento_id = g.pagamento_id GROUP BY k", "k"),
        esp["por_metodo"])

    comparar_agregado(rel, "receita por dia da semana (1=seg..7=dom)", agregado(
        "SELECT d.dia_semana k, SUM(f.receita) r FROM facto_vendas f "
        "JOIN dim_data d ON f.data_id = d.data_id GROUP BY k", "k"),
        esp["por_dia_semana"])

    n = spark.sql("SELECT COUNT(DISTINCT pedido_id) n FROM facto_vendas").first()["n"]
    rel.checar(f"pedidos distintos ({esp['pedidos_distintos']})",
               n == esp["pedidos_distintos"], f"obtido {n}")

    print()
    print("=" * 66)
    print(f"  falhas: {rel.falhas}      notas: {rel.notas}")
    print("=" * 66)
    print()

    spark.stop()
    return 0 if rel.falhas == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
