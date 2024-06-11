from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_sel_4037(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_sel_4037}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_sel_4037}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_sel_4037}")\
        .option(
          "query",
          """select
\tp.proc_cd_sk,
\tp.proc_cd_desc

from
\tprod.proc_cd_d p

where
\tp.proc_cd_desc like '%depressive%'
\tor p.proc_cd_desc like '%mood%'
\tor p.proc_cd_desc like '%depression%'
\tor p.proc_cd_desc like '%depressant%'"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
