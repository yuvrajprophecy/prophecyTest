from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_4070(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("`Received Date`").alias("Received Date"))

    return df1.agg(
        sum(col("MemberPay")).alias("Sum_MemberPay"), 
        sum(col("RetailCost")).alias("Sum_RetailCost"), 
        count_distinct(col("NDC_SK"), lit(0)).alias("CountDistinct_NDC_SK"), 
        count_distinct(col("`Diagnosis Type`"), lit(0)).alias("CountDistinct_Diagnosis Type"), 
        sum(col("GenericIndicator")).alias("Sum_GenericIndicator"), 
        sum(col("IngredientCost")).alias("Sum_IngredientCost"), 
        sum(col("MailOrder")).alias("Sum_MailOrder"), 
        sum(col("SpecialtyIndicator")).alias("Sum_SpecialtyIndicator")
    )
