from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def test_live_visit_2_cs(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ED Visit Alw Amt", StringType(), True), StructField("MBR_INDV_BE_KEY", StringType(), True), StructField("Household ED Visit Tot Alw Amt", StringType(), True), StructField("Diag Codes", StringType(), True), StructField("PCMH Attributed", StringType(), True), StructField("Target", StringType(), True), StructField("PCP Attributed", StringType(), True), StructField("positive_probability", StringType(), True), StructField("Max Claim Line Spend", StringType(), True), StructField("prediction_threshold", StringType(), True), StructField("Total Claim Spend", StringType(), True), StructField("PR", StringType(), True), StructField("ZIP", StringType(), True), StructField("Index Month", StringType(), True), StructField("Drug/Alcohol/Psych Related ED Visit", StringType(), True), StructField("Last_YMD", StringType(), True), StructField("Group ID", StringType(), True), StructField("Hospital Stay Days", StringType(), True), StructField("Service Area", StringType(), True), StructField("CCI Score", StringType(), True), StructField("Non Emergent Percentage", StringType(), True), StructField("Medical Home (str)", StringType(), True), StructField("Coverage Area", StringType(), True), StructField("prediction", StringType(), True), StructField("Industry", StringType(), True), StructField("Population", StringType(), True), StructField("Right_MBR_INDV_BE_KEY", StringType(), True), StructField("OP", StringType(), True), StructField("class_1.0", StringType(), True), StructField("Household ED Visits", StringType(), True), StructField("Members in Household", StringType(), True), StructField("row_id", StringType(), True), StructField("Household ED Visit Alw Amt", StringType(), True), StructField("ED Visits", StringType(), True), StructField("IP", StringType(), True), StructField("Last_Week", StringType(), True), StructField("class_0.0", StringType(), True), StructField("Drug Counts", StringType(), True), StructField("CountDistinct_Week", StringType(), True), StructField("Mental Health Claims", StringType(), True), StructField("Historic Non Emergent Visit", StringType(), True), StructField("Injury Related ED", StringType(), True), StructField("ED Visited", StringType(), True), StructField("Claims", StringType(), True), StructField("Relationship", StringType(), True), StructField("Drug Related ED", StringType(), True), StructField("Drug Classes", StringType(), True), StructField("Polypharmacy", StringType(), True), StructField("Right_CountDistinct_Week", StringType(), True), StructField("Psych Related ED", StringType(), True), StructField("New Drugs", StringType(), True), StructField("Gender", StringType(), True), StructField("Distance to Closest ED", StringType(), True), StructField("Race", StringType(), True), StructField("Member Age", StringType(), True), StructField("SPIRA Elligible", StringType(), True), StructField("Medical Home", StringType(), True), StructField("7-12 Month ED Visit History", StringType(), True), StructField("ED Visit Tot Alw Amt", StringType(), True), StructField("FREQUENT_FLYER", StringType(), True), StructField("Alcohol Related ED", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\test_live_visit_2.csv")
