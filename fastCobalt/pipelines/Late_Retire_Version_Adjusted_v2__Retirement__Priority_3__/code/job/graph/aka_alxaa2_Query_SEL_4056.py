from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_SEL_4056(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_SEL_4056}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_SEL_4056}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_SEL_4056}")\
        .option(
          "query",
          """SELECT DISTINCT 
proc_cd_sk, proc_cd, proc_cd_typ_cd, proc_cd_desc, proc_cd_term_dt_sk
FROM PROC_CD_D"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
