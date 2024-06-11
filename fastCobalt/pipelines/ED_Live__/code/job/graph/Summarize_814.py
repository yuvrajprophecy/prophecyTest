from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_814(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("YR_MO"), col("MBR_INDV_BE_KEY"))

    return df1.agg(
        sum(col("DRUG_CLASS_COUNT")).alias("DRUG_CLASS_COUNT"), 
        sum(col("DRUG_COUNT")).alias("DRUG_COUNT"), 
        sum(col("POLYPHARMACY_IN")).alias("POLYPHARMACY_IN")
    )
