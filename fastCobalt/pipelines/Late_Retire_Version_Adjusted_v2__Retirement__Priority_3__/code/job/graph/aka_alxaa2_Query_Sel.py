from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_Sel(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_Sel}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_Sel}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_Sel}")\
        .option(
          "query",
          """Select distinct
\tM.MBR_INDV_BE_KEY,
\tM.MBR_RELSHP_NM,
\tM.MBR_UNIQ_KEY,
\trcst.SUB_UNIQ_KEY,
\tM.MBR_SK,
\tM.MBR_BRTH_DT_SK,
\tyr_mo.first_dt_of_mo

From 
\tPROD.MBR_rcst_ct_f rcst
\tInner Join 
\t\tPROD.MBR_D M On M.MBR_SK = rcst.MBR_SK 
\t--inner join
\t--\tprod.mbr_enr_d e on e.mbr_sk = rcst.mbr_sk
\tinner join
\t\tprod.yr_mo_d yr_mo on yr_mo.yr_mo_sk = rcst.actvty_yr_mo_sk

Where 
\tyr_mo.first_dt_of_mo > char(current date - 49 month, iso)
\tand yr_mo.first_dt_of_mo < char(current date - 1 month, iso)
\tAnd M.MBR_INDV_BE_KEY <> '0'"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
