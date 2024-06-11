from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_s(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_s}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_s}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_s}")\
        .option(
          "query",
          """select MBR_D.MBR_INDV_BE_KEY,
\tLAB_RSLT_F.ORDER_TST_NM,
\tLAB_RSLT_F.LAB_RSLT_SVC_DT_SK,
\tLeft(LAB_RSLT_F.LAB_RSLT_SVC_DT_SK, 7) as YEARMONTH,
\tLAB_RSLT_F.LAB_RSLT_NORM_HI_VAL,
\tLAB_RSLT_F.LAB_RSLT_NORM_LOW_VAL,
\tLAB_RSLT_F.LAB_RSLT_NUM_RSLT_VAL,
\tLAB_RSLT_F.LAB_RSLT_DESC,
\tLAB_RSLT_F.LAB_RSLT_CLS_NM,
\tLAB_RSLT_F.LOINC_CD_SK,
\tLOINC_CD 
from LAB_RSLT_F 
\tinner join MBR_D on MBR_D.MBR_SK = LAB_RSLT_F.MBR_SK 
where LAB_RSLT_F.LAB_RSLT_SVC_DT_SK Between char(((Current Date + 1 Days - Day(Current Date) Days) - 60 Months), ISO) And CHAR((LAST_DAY(Current Date - 13 Months)), ISO)"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
