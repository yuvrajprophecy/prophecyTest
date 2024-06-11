from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_sel_2772(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_sel_2772}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_sel_2772}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_sel_2772}")\
        .option(
          "query",
          """select
\tm.mbr_indv_be_key \"Member BE Key\",
\tsp.mbr_indv_be_key \"Household Member BE Key\",
\trcst.prod_sh_nm \"Member Product\",
\tsp.prod_sh_nm \"Household Member Prod\",
\trcst.actvty_yr_mo_sk,
\tsp.retiree_in_household_in

from
\tprod.mbr_d m
\tinner join
\t\tprod.mbr_rcst_ct_f rcst on rcst.mbr_sk = m.mbr_sk
\tinner join
\t\tprod.yr_mo_d yr_mo on yr_mo.yr_mo_sk = rcst.actvty_yr_mo_sk
\tinner join
\t\tprod.mbr_enr_d e on e.mbr_sk = rcst.mbr_sk
\tinner join
\t\t(
\t\t\tselect
\t\t\t\tm.mbr_indv_be_key,
\t\t\t\tm.mbr_home_addr_ln_1,
\t\t\t\tm.mbr_home_addr_ln_2,
\t\t\t\tm.mbr_home_addr_cnty_nm,
\t\t\t\tm.mbr_home_addr_st_cd,
\t\t\t\trcst.prod_sh_nm,
\t\t\t\trcst.actvty_yr_mo_sk,
\t\t\t\t1 as retiree_in_household_in

\t\t\tfrom
\t\t\t\tprod.mbr_d m
\t\t\t\tinner join
\t\t\t\t\tprod.mbr_rcst_ct_f rcst on rcst.mbr_sk = m.mbr_sk
\t\t\t\tinner join
\t\t\t\t\tprod.mbr_enr_d e on e.mbr_sk = rcst.mbr_sk
\t\t\t\tinner join
\t\t\t\t\tprod.yr_mo_d yr_mo on yr_mo.yr_mo_sk = rcst.actvty_yr_mo_sk

\t\t\twhere
\t\t\t\trcst.prod_sh_nm in ('BMADVP','BMADVH','MEDGAP','MEDSEL','TIP','CBLU65')
\t\t\t\tand m.mbr_indv_be_key <> 0
\t\t\t\tand yr_mo.first_dt_of_mo > char(current date - 49 month, iso)
\t\t\t\tand yr_mo.first_dt_of_mo < char(current date - 1 month, iso)
\t\t\t\tand rcst.mbr_age_at_actvty_yr_mo >= 65
\t\t\t\tand e.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
\t\t) sp on sp.actvty_yr_mo_sk = rcst.actvty_yr_mo_sk and sp.MBR_HOME_ADDR_LN_1 = m.MBR_HOME_ADDR_LN_1 and sp.mbr_home_addr_cnty_nm = m.mbr_home_addr_cnty_nm and sp.mbr_home_addr_st_cd = m.mbr_home_addr_st_cd and sp.mbr_home_addr_ln_2 = m.mbr_home_addr_ln_2

where
\tm.mbr_indv_be_key <> 0
\tand rcst.prod_sh_nm not in ('BMADVP','BMADVH','MEDGAP','MEDSEL','TIP','CBLU65')
\tand rcst.prod_sh_nm not like '%DENT%'
\tand e.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
\tand m.mbr_indv_be_key <> sp.mbr_indv_be_key
\tand yr_mo.first_dt_of_mo > char(current date - 49 month, iso)
\tand yr_mo.first_dt_of_mo < char(current date - 1 month, iso)
\tand rcst.mbr_age_at_actvty_yr_mo >= 65"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
