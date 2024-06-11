from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_496(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_UNIQ_KEY"), 
        col("PROV_PRI_PRCTC_ADDR_ST_CD"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("MBR_HOME_ADDR_LN_1"), 
        col("DistanceMiles"), 
        col("MBR_INDV_BE_KEY"), 
        col("PROV_PRI_PRCTC_ADDR_LN_1"), 
        col("PROV_PRI_PRCTC_ADDR_ZIP_CD_5"), 
        col("PROV_NM"), 
        col("MBR_HOME_ADDR_ST_CD"), 
        col("MBR_HOME_ADDR_CITY_NM"), 
        col("PROV_PRI_PRCTC_ADDR_CITY_NM")
    )
