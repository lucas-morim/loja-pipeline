# Pipeline de dados — loja online

Pipeline de dados completo em AWS, definido inteiramente em código com CDK.
Lê os extractos diários de uma loja online em CSV, transforma-os num modelo
dimensional em estrela, e disponibiliza-os para consulta em SQL.

```
                 ┌──────────────┐
   CSV  ────────▶│  S3  raw/    │
   (extracto     └──────┬───────┘
    da loja)            │
                        ▼
                 ┌──────────────┐              ┌─────────────────┐
                 │  Glue job    │◀─────────────│     Airflow     │
                 │  loja-etl    │   dispara    │  loja_pipeline  │
                 └──────┬───────┘              │                 │
                        ▼                      │  diaria, 03:00  │
                 ┌──────────────────────┐      │  encadeia os    │
                 │  S3  processado/     │      │  dois passos e  │
                 └──────┬───────────────┘      │  trata de falhas│
                        ▼                      │                 │
                 ┌──────────────┐   dispara    │                 │
                 │ Glue Catalog │◀─────────────│                 │
                 └──────┬───────┘              └─────────────────┘
                        ▼
                 ┌──────────────┐
                 │   Athena     │   SQL sobre ficheiros, sem servidor
                 └──────────────┘
```

O Glue corre o PySpark que limpa e modela; o resultado é Parquet particionado
por ano e mês; o catálogo guarda o esquema e as localizações; o Athena consulta
por cima. O Airflow não processa nada — manda arrancar, espera, e só cataloga
se a transformação tiver corrido bem.

## O modelo

Esquema em estrela, com o grão **uma linha por produto dentro de um pedido
concluído** — 388 linhas de factos a partir de 180 pedidos.

```
              dim_data
                  │
   dim_produto ── facto_vendas ── dim_cliente
                  │
             dim_pagamento
```

| Tabela | Grão | Linhas |
| --- | --- | --- |
| `facto_vendas` | um produto dentro de um pedido concluído | 388 |
| `dim_data` | um dia | 81 |
| `dim_produto` | um produto | 14 |
| `dim_cliente` | um cliente (+ o membro desconhecido) | 31 |
| `dim_pagamento` | um método de pagamento | 4 |

A tabela de factos tem apenas dois tipos de coluna: **chaves**, pelas quais se
corta, e **medidas**, que se somam. Nenhuma coluna de texto, nenhuma chave nula.

## Decisões, e porquê

**O grão é o item, não o pedido.** Com uma linha por pedido o total da receita
continuaria certo, mas seria impossível responder a "receita por categoria" —
um pedido pode ter produtos de categorias diferentes. Escolhe-se sempre o grão
mais fino: agrega-se para cima, nunca para baixo.

**A categoria vive dentro de `dim_produto`, não numa dimensão própria.**
Repetir o texto `"Informatica"` em catorze linhas custa bytes irrelevantes e
poupa um salto em todas as consultas. Desnormaliza-se nas dimensões,
normaliza-se nos factos.

**O desconto não é uma medida.** É uma fracção, e somar fracções não dá nada
interpretável. Entra no cálculo da receita durante o ETL e desaparece; o que
fica guardado é o valor em euros, que é aditivo.

**Existe um membro desconhecido** (`cliente_id = -1`). Há pedidos de convidado,
sem conta associada. Uma chave nula na tabela de factos faria o `JOIN` descartar
essas linhas em silêncio — cerca de 1400 € que desapareceriam de todos os
relatórios por distrito sem ninguém dar por isso.

**As dimensões juntam-se com `LEFT JOIN`, nunca `INNER`.** Se uma dimensão
estiver incompleta, as linhas de factos sobrevivem com a chave a nulo e o erro
é visível. Com `INNER JOIN` as linhas desaparecem e os totais mudam sem aviso.
Um pipeline fiável não é um que não falha — é um em que as falhas não passam
despercebidas.

**A tabela de factos é particionada por ano e mês.** O Athena cobra por byte
lido. Com as partições nos nomes das pastas, uma consulta a um mês lê um
ficheiro em vez de todos.

**A transformação corre em Glue, não em Lambda.** O volume aqui não o justifica,
mas o código é o mesmo que correria com milhões de linhas, e uma Lambda tem
limites de memória e de tempo que o tornariam impossível.

**O script de transformação não fixa o `master` do Spark.** Localmente o Spark
assume `local[*]`; no Glue usa o cluster. Fixá-lo faria o job correr numa só
máquina sem dar erro nenhum.

## Correr

Requisitos: AWS CLI configurado, Node.js, Python 3.11+, e o CDK
(`npm install -g aws-cdk`).

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cdk bootstrap            # uma vez por conta e região
cdk deploy
```

Lançar o ETL e catalogar o resultado, à mão:

```bash
aws glue start-job-run --job-name loja-etl
aws glue start-crawler --name loja-crawler
```

Ou deixar o Airflow fazê-lo (ver *Orquestração*, abaixo).

Depois, no Athena, workgroup `loja`, base de dados `loja`:

```sql
SELECT p.categoria, ROUND(SUM(f.receita), 2) AS receita
FROM facto_vendas f
JOIN dim_produto p ON f.produto_id = p.produto_id
GROUP BY p.categoria
ORDER BY receita DESC
```

Para desmontar tudo:

```bash
cdk destroy
```

### Correr a transformação localmente

O mesmo script corre sem AWS nenhuma, sobre os CSV em `dados/`:

```bash
cd jobs
python transformar.py
python verificar_spark.py
```

O verificador corre o ETL, lê o Parquet que saiu, e compara com valores
calculados outra vez em Python puro a partir dos CSV — dois caminhos
independentes.

## Orquestração

A DAG `dags/loja_pipeline_dag.py` encadeia os dois passos, todos os dias às
03:00:

```
transformar  ──▶  catalogar
```

Se a transformação falhar, o catalogador não corre — que é a razão de existir
um orquestrador. Sem ele, o crawler corria na mesma e catalogava o resultado
de uma transformação falhada, deixando o Athena a servir dados errados sem
nenhum sinal de que algo correu mal.

O Airflow apenas **dispara** os recursos; quem os **define** é o CDK. Um
recurso declarado em dois sítios acaba por divergir, e deixa de se saber qual
manda. É também por isso que o passo do crawler usa uma chamada directa à API
em vez do `GlueCrawlerOperator`: esse operador quer a definição completa do
crawler, porque também o sabe criar.

Para correr localmente:

```bash
pip install apache-airflow-providers-amazon
ln -s "$(pwd)/dags/loja_pipeline_dag.py" "$AIRFLOW_HOME/dags/"
airflow standalone
```

As credenciais vêm da cadeia por omissão do boto3, ou seja, do mesmo
`~/.aws/credentials` que o AWS CLI usa.

## Estrutura

```
app.py                      ponto de entrada do CDK
loja_pipeline/
  loja_pipeline_stack.py    a infraestrutura: bucket, job, catálogo, Athena
jobs/
  transformar.py            o ETL em PySpark (corre local e no Glue)
  verificar_spark.py        testes do ETL contra um caminho independente
dags/
  loja_pipeline_dag.py      a orquestração
dados/                      os CSV de exemplo
```

## O que não está pronto para produção

Decisões conscientes, tomadas para o projecto ser reproduzível e pequeno:

**Os dados de exemplo são carregados pelo CDK.** Infraestrutura e dados têm
ciclos de vida diferentes e não deviam viajar juntos. Aqui é deliberado: quem
clonar o repositório tem o pipeline a funcionar sem arranjar dados. Num sistema
real os ficheiros chegariam por um processo de ingestão à parte.

**As partições estão tipadas como texto.** Vêm dos nomes das pastas, e o crawler
não tem como saber que `2026` é um número. A solução é *partition projection*:
declarar a regra das partições em vez de as descobrir, o que corrige os tipos e
dispensa o crawler quando entra um mês novo.

**As chaves substitutas são regeneradas a cada execução.** O `pagamento_id` sai
de um `row_number()` sobre os métodos ordenados. Se aparecer um método novo, a
numeração dança e os factos antigos passam a apontar para o método errado. Em
produção mantém-se uma tabela de correspondência persistente.

**O dinheiro está em vírgula flutuante.** Isso faz com que motores diferentes
discordem num cêntimo quando a soma cai em cima do meio cêntimo — aconteceu
neste conjunto de dados, entre o SQLite e o Spark. A resposta correcta é um
tipo decimal.

**Cada execução reescreve tudo.** Não há carga incremental nem marcadores de
posição. Com este volume é instantâneo; com anos de histórico, não seria
aceitável.

**O Airflow corre localmente**, não na nuvem. Para o pipeline correr sem o meu
portátil ligado, a DAG teria de viver num Airflow gerido — na AWS, o MWAA — ou
num servidor próprio. A DAG em si não mudaria.

**Não há testes de qualidade de dados** no pipeline — só os testes do ETL,
que correm localmente. Em produção verificar-se-iam contagens, nulos e
intervalos a cada execução, com o pipeline a parar quando algo fugisse.

**O papel de execução do CloudFormation tem permissões de administrador**, que é
o que o `cdk bootstrap` faz por omissão. Numa empresa restringir-se-ia ao
mínimo necessário.