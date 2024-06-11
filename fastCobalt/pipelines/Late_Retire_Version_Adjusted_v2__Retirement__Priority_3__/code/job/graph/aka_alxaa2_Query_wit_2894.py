from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_wit_2894(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_wit_2894}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_wit_2894}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_wit_2894}")\
        .option(
          "query",
          """with addresses as --Select unique complete line addresses for a member, including any address registered under this member 
(
\tselect distinct
\t\tm.mbr_home_addr_ln_1 || m.mbr_home_addr_ln_2 || m.mbr_home_addr_cnty_nm as addr_complete,
\t\tm.mbr_home_addr_ln_1 || ' ' || m.mbr_home_addr_cnty_nm || ' ' || m.mbr_home_addr_city_nm || ' ' || m.mbr_home_addr_st_cd as addr_complete_apt_fixed,
\t\tm.mbr_indv_be_key,
\t\tm.mbr_sk

\tfrom
\t\tprod.mbr_d m

\twhere
\t\tm.mbr_indv_be_key != 0
),

members_per_address as -- Grouping by each address and the date associated within BlueKC for the address, count the number of members living there
(
\tselect
\t\ta.addr_complete,
\t\ta.addr_complete_apt_fixed,
\t\tyr_mo.first_dt_of_mo,
\t\tcount( distinct m.mbr_indv_be_key) as residents

\tfrom
\t\taddresses a
\t\tinner join
\t\t\tprod.mbr_d m on m.mbr_indv_be_key = a.mbr_indv_be_key and m.mbr_sk = a.mbr_sk
\t\tinner join
\t\t\tprod.mbr_rcst_ct_f rcst on rcst.mbr_sk = a.mbr_sk
\t\tinner join
\t\t\tprod.yr_mo_d yr_mo on yr_mo.yr_mo_sk = rcst.actvty_yr_mo_sk

\twhere
\t\tyr_mo.first_dt_of_mo >= char(current date - 49 month, iso)
\t\tand yr_mo.first_dt_of_mo <= char(current date -1 month, iso)

\tgroup by
\t\ta.addr_complete,
\t\ta.addr_complete_apt_fixed,
\t\tyr_mo.first_dt_of_mo
)

select
\ta.mbr_indv_be_key,
\ta.mbr_sk,
\tmpa.first_dt_of_mo,
\tmpa.residents,
\ta.addr_complete_apt_fixed

from
\taddresses a
\tinner join
\t\tmembers_per_address mpa on mpa.addr_complete = a.addr_complete

where
\tmpa.residents <= 12"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
