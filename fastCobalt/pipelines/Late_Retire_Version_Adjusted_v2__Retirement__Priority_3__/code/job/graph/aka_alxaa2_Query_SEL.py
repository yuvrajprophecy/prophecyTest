from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_SEL(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_SEL}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_SEL}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_SEL}")\
        .option(
          "query",
          """SELECT 
rcst.actvty_yr_mo_sk,
m.mbr_indv_be_key,
g.grp_id,
g.grp_nm,
m.mbr_gndr_cd,
rcst.mbr_age_at_actvty_yr_mo

from
\tprod.mbr_rcst_ct_f rcst
\tinner join
\t\tprod.yr_mo_d yr_mo on yr_mo.yr_mo_sk = rcst.actvty_yr_mo_sk
\tinner join
\t\tprod.mbr_d m on m.mbr_sk = rcst.mbr_sk
\tinner join
\t\tprod.grp_d g on g.grp_sk = rcst.grp_sk

where
\tm.mbr_indv_be_key <> 0
\tand g.grp_dp_in = 'N'
\tand g.grp_clnt_id != 'MA'
\t--and g.grp_sttus_cd != 'TERM'
\tand m.host_mbr_in = 'N'
\tand g.grp_id not in ('10003000','10001000')
\tand left(g.grp_id, 2) != '65'
\tand YR_MO.FIRST_DT_OF_MO <= CHAR((LAST_Day(Current Date - 1 Months)), ISO) 
\tand YR_MO.FIRST_DT_OF_MO >= CHAR((LAST_Day(Current Date - 49 Months)), ISO) 
\tand m.mbr_relshp_cd = 'SUB'"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
