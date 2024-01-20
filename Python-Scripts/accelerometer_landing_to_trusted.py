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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1705552057067 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1705552057067",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705552228812 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1705552228812",
)

# Script generated for node Transform SQL
SqlQuery1941 = """
select distinct al.user, al.timestamp, al.x, al.y, al.z 
from accelerometer_landing al 
join customer_trusted ct 
on al.user = ct.email
;

"""
TransformSQL_node1705552143432 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1941,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1705552057067,
        "customer_trusted": CustomerTrusted_node1705552228812,
    },
    transformation_ctx="TransformSQL_node1705552143432",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705555361288 = glueContext.getSink(
    path="s3://stedi-project-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1705555361288",
)
AccelerometerTrusted_node1705555361288.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1705555361288.setFormat("json")
AccelerometerTrusted_node1705555361288.writeFrame(TransformSQL_node1705552143432)
job.commit()
