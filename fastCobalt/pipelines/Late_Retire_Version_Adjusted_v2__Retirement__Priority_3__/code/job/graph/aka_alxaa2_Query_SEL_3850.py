from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_SEL_3850(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_SEL_3850}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_SEL_3850}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_SEL_3850}")\
        .option("query", """SELECT
\tm.mbr_indv_be_key,
\tm.mbr_id

from
\tprod.mbr_d m""")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
