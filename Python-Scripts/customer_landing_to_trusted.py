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

# Script generated for node Customer Landing
CustomerLanding_node1705550298640 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1705550298640",
)

# Script generated for node Tranform SQL
SqlQuery1538 = """
select * from customer_landing
where shareWithResearchAsOfDate is not null; 

"""
TranformSQL_node1705550527139 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1538,
    mapping={"customer_landing": CustomerLanding_node1705550298640},
    transformation_ctx="TranformSQL_node1705550527139",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705551072264 = glueContext.getSink(
    path="s3://stedi-project-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1705551072264",
)
CustomerTrusted_node1705551072264.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="customer_trusted"
)
CustomerTrusted_node1705551072264.setFormat("json")
CustomerTrusted_node1705551072264.writeFrame(TranformSQL_node1705550527139)
job.commit()
