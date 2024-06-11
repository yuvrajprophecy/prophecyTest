from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_390(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("Week"))

    return df1.agg(
        max(col("psych")).alias("Max_psych"), 
        avg(col("`ER Severity`")).alias("Avg_ER Severity"), 
        max(col("drug")).alias("Max_drug"), 
        max(col("alcohol")).alias("Max_alcohol"), 
        max(col("injury")).alias("Max_injury")
    )
