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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705622478844 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1705622478844",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705622473309 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1705622473309",
)

# Script generated for node Transform SQL
SqlQuery1909 = """
select distinct ct.*
from customer_trusted ct
join accelerometer_trusted at 
on ct.email = at.user
"""
TransformSQL_node1705622570871 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1909,
    mapping={
        "customer_trusted": CustomerTrusted_node1705622473309,
        "accelerometer_trusted": AccelerometerTrusted_node1705622478844,
    },
    transformation_ctx="TransformSQL_node1705622570871",
)

# Script generated for node Customer Curated
CustomerCurated_node1705622979430 = glueContext.getSink(
    path="s3://stedi-project-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1705622979430",
)
CustomerCurated_node1705622979430.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="customer_curated"
)
CustomerCurated_node1705622979430.setFormat("json")
CustomerCurated_node1705622979430.writeFrame(TransformSQL_node1705622570871)
job.commit()
