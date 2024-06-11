from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_wit_2972(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_wit_2972}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_wit_2972}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_wit_2972}")\
        .option(
          "query",
          """with filtered_members as 
(
\tSelect distinct
\t\tm.mbr_indv_be_key,
\t\tm.mbr_sk,
\t\tmax(rcst.actvty_yr_mo_sk) as last_membermonth

\tFROM 
\t\tprod.mbr_d m
\t\tinner join
\t\t\tprod.mbr_rcst_ct_f rcst on rcst.mbr_sk = m.mbr_sk

\tWHERE
\t\trcst.actvty_yr_mo_sk > 201809
\t\tand rcst.orig_exprnc_cat_cd != 'FEP'
\t\tand rcst.mbr_age_at_actvty_yr_mo >= 63

\tgroup by
\t\tm.mbr_indv_be_key,
\t\tm.mbr_sk
),

formated_members as 
(
\tselect distinct
\t\tfm.mbr_indv_be_key,
\t\tmax(fm.last_membermonth) as max_month

\tfrom
\t\tfiltered_members fm

\tgroup by
\t\tfm.mbr_indv_be_key
)

select 
\tfom.mbr_indv_be_key,
\trcst.actvty_yr_mo_sk

from
\tformated_members fom
\tinner join
\t\tfiltered_members fim on fim.mbr_indv_be_key = fom.mbr_indv_be_key and fim.last_membermonth = fom.max_month
\tinner join
\t\tprod.mbr_rcst_ct_f rcst on rcst.mbr_sk = fim.mbr_sk"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
