from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_sel_4038(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_sel_4038}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_sel_4038}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_sel_4038}")\
        .option(
          "query",
          """select
\td.diag_cd_sk,
\td.diag_cd_desc

from
\tprod.diag_cd_d d

where
\td.diag_cd_desc like '%depression%'
\tor d.diag_cd_desc like '%depressive%'"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
