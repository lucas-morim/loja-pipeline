from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct


class LojaPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dados = s3.Bucket(
            self, "Dados",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        s3deploy.BucketDeployment(
            self, "DadosIniciais",
            sources=[s3deploy.Source.asset("dados")],
            destination_bucket=self.dados,
            destination_key_prefix="raw/",
        )

        papel = iam.Role(
            self, "PapelEtl",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"),
            ],
        )
        self.dados.grant_read_write(papel)

        self.job = glue.PySparkEtlJob(
            self, "JobEtl",
            job_name="loja-etl",
            role=papel,
            script=glue.Code.from_asset("jobs/transformar.py"),
            glue_version=glue.GlueVersion.V6_0,
            worker_configuration=glue.WorkerConfiguration(
                worker_type=glue.WorkerType.G_1X,
                number_of_workers=2,
            ),
            default_arguments={
                "--entrada": f"s3://{self.dados.bucket_name}/raw",
                "--saida": f"s3://{self.dados.bucket_name}/processado",
            },
        )

        base = glue.CfnDatabase(
            self, "BaseDados",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="loja"),
        )

        glue.CfnCrawler(
            self, "Catalogador",
            name="loja-crawler",
            role=papel.role_arn,
            database_name="loja",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{self.dados.bucket_name}/processado/")],
            ),
        ).add_depends_on(base)

        athena.CfnWorkGroup(
            self, "GrupoAthena",
            name="loja",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.dados.bucket_name}/athena-resultados/",
                ),
            ),
        )