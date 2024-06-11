from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_76_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.YEARMONTH") == col("in1.YEARMONTH"))),
          "leftouter"
        )\
        .select(col("in0.MEMBER_AGE2").alias("MEMBER_AGE2"), col("in0.IP_Running6").alias("IP_Running6"), col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.Relative_IP_Month").alias("Relative_IP_Month"), col("in0.IP_Running3").alias("IP_Running3"), col("in0.IP_Running3_Max").alias("IP_Running3_Max"), col("in1.UniqueLabs_Running3").alias("UniqueLabs_Running3"), col("in0.MEMBER_AGE").alias("MEMBER_AGE"), col("in0.First_IP").alias("First_IP"), col("in0.IP_Running6_Max").alias("IP_Running6_Max"), col("in0.IP_PreviousMonth").alias("IP_PreviousMonth"), col("in1.CountOfLabs_Running3").alias("CountOfLabs_Running3"), col("in0.IP_Month_Number").alias("IP_Month_Number"), col("in1.Concat_ORDER_TST_NM_Running3").alias("Concat_ORDER_TST_NM_Running3"), col("in0.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in1.HighestAlbumin_Running3").alias("HighestAlbumin_Running3"), col("in1.LowestGFR_Running3").alias("LowestGFR_Running3"), col("in0.TOT_MONTHS").alias("TOT_MONTHS"), col("in0.RecordID").alias("RecordID"), col("in0.IP_Record").alias("IP_Record"))
