import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1705625242711 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1705625242711",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1705625182585 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1705625182585",
)

# Script generated for node Transform SQL
SqlQuery2317 = """
select distinct sl.* 
from step_trainer_landing sl  
join customer_curated cc 
on sl.serialnumber = cc.serialnumber;
"""
TransformSQL_node1705625283610 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2317,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1705625182585,
        "customer_curated": CustomerCurated_node1705625242711,
    },
    transformation_ctx="TransformSQL_node1705625283610",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705625831880 = glueContext.getSink(
    path="s3://stedi-project-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1705625831880",
)
StepTrainerTrusted_node1705625831880.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1705625831880.setFormat("json")
StepTrainerTrusted_node1705625831880.writeFrame(TransformSQL_node1705625283610)
job.commit()
