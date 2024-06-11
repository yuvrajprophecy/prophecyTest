from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_sel_3842(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_sel_3842}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_sel_3842}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_sel_3842}")\
        .option(
          "query",
          "select grp_nm,grp_id,grp_sk from prod.grp_d where grp_d.grp_clnt_id = 'MA' and grp_d.grp_dp_in = 'N'"
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
