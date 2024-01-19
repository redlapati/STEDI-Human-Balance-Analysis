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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705627206773 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1705627206773",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705627208544 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1705627208544",
)

# Script generated for node Transform SQL
SqlQuery2013 = """
select st.serialnumber, st.sensorreadingtime, st.distancefromobject,
       at.timestamp, at.x, at.y, at.z    
from step_trainer_trusted st 
join accelerometer_trusted at 
on st.sensorreadingtime = at.timestamp;

"""
TransformSQL_node1705627314931 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2013,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1705627206773,
        "accelerometer_trusted": AccelerometerTrusted_node1705627208544,
    },
    transformation_ctx="TransformSQL_node1705627314931",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1705627730529 = glueContext.getSink(
    path="s3://stedi-project-lake-house/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1705627730529",
)
MachineLearningCurated_node1705627730529.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1705627730529.setFormat("json")
MachineLearningCurated_node1705627730529.writeFrame(TransformSQL_node1705627314931)
job.commit()
