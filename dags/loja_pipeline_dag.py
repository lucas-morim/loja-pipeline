"""
Orquestracao do pipeline da loja.

    transformar  ->  catalogar

O Airflow nao processa dados nenhuns aqui. Manda o Glue arrancar, espera,
e so depois manda catalogar. Se o primeiro falhar, o segundo nao corre --
que e precisamente a razao de existir um orquestrador: sem ele, o crawler
corria na mesma e catalogava o resultado de uma transformacao falhada.

Os recursos (o job, o crawler, o bucket) estao definidos no CDK. Esta DAG
so os dispara. Nao os cria nem os configura, de proposito: um recurso
definido em dois sitios acaba por divergir.
"""

from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import dag, task

REGIAO = "eu-west-1"
JOB = "loja-etl"
CRAWLER = "loja-crawler"


@dag(
    dag_id="loja_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule="0 3 * * *",
    catchup=False,
    tags=["loja", "aws"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
)
def loja_pipeline():

    transformar = GlueJobOperator(
        task_id="transformar",
        job_name=JOB,
        region_name=REGIAO,
        wait_for_completion=True,
    )

    @task
    def catalogar():
        """Corre o crawler e espera que ele acabe.

        Ha um GlueCrawlerOperator, mas ele quer a definicao completa do
        crawler porque tambem o sabe criar. Como o nosso vive no CDK,
        usamos a API directamente: a DAG dispara, nao define.

        Nota: este ciclo ocupa um lugar de execucao enquanto espera. Com
        esperas longas usar-se-ia um sensor em modo deferrable, que liberta
        o lugar entre verificacoes.
        """
        import time

        import boto3

        glue = boto3.client("glue", region_name=REGIAO)
        glue.start_crawler(Name=CRAWLER)

        limite = time.monotonic() + 15 * 60
        while time.monotonic() < limite:
            crawler = glue.get_crawler(Name=CRAWLER)["Crawler"]
            if crawler["State"] == "READY":
                estado = crawler.get("LastCrawl", {}).get("Status")
                if estado != "SUCCEEDED":
                    raise RuntimeError(f"o crawler acabou em {estado}")
                print(f"crawler {CRAWLER}: {estado}")
                return
            time.sleep(15)

        raise TimeoutError(f"o crawler {CRAWLER} nao acabou em 15 minutos")

    transformar >> catalogar()


loja_pipeline()
