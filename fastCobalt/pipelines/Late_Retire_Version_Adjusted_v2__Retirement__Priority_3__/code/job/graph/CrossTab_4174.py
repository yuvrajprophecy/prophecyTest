from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_4174(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("YEARMONTH"))
    df2 = df1.pivot(
        "Product",
        ["Avg_PCB",  "Avg_BCARE",  "Avg_HPEXTRNL",  "Avg_BLUESELECT_",  "Avg_PC",  "Avg_BLUE_SELECT",  "Avg_BLUE_ACCESS",          "Avg_PCBEXTRNL"]
    )

    return df2.agg(avg(col("`Product Counter`")).alias("Product Counter"))
