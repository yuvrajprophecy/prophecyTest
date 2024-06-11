from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_sel(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_sel}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_sel}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_sel}")\
        .option(
          "query",
          """select
\tm.mbr_indv_be_key,
\tyr_mo.first_dt_of_mo,
\tm.sub_sk,
\trcst.prod_sh_nm,
\tm.mbr_relshp_cd

from
\tprod.mbr_rcst_ct_f rcst
\tinner join
\t\tprod.mbr_d m on m.mbr_sk = rcst.mbr_sk
\tinner join
\t\tprod.yr_mo_d yr_mo on yr_mo.yr_mo_sk = rcst.actvty_yr_mo_sk

where
\tm.mbr_relshp_cd in ('SPOUSE','SUB')
\tand rcst.prod_sh_nm in ('PCB','BCARE','BLUE-SELECT','BLUESELECT+','BLUE-ACCESS','PC','HP','BMADVP','BMADVH','MEDGAP','MEDSUP','MEDSEL','TIP','BLU65')
\tand yr_mo.first_dt_of_mo > char(current date - 49 month, iso)
\tand yr_mo.first_dt_of_mo < char(current date - 1 month, iso)"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
