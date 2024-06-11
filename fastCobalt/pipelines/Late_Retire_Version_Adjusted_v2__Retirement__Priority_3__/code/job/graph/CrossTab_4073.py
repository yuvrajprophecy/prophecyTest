from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_4073(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("`Received Date`").alias("Received Date"))
    df2 = df1.pivot(
        "`Diagnosis Type`",
        ["EYE", "NO_DIAGNOSTIC_CATEGORY", "ENDOCRINE__NUTRITIONAL_AND_METABOLIC", "FACTORS_INFLUENCING_HEALTH_STATUS"]
    )

    return df2.agg(sum(col("counter")).alias("counter"))
