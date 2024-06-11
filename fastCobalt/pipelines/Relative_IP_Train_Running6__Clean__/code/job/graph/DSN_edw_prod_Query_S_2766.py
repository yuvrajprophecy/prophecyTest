from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_S_2766(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_S_2766}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_S_2766}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_S_2766}")\
        .option(
          "query",
          """Select distinct left(CHAR(YR_MO_D.FIRST_DT_OF_MO), 7)
From PROD.YR_MO_D
WHERE YR_MO_D.FIRST_DT_OF_MO Between CHAR(((Current Date + 1 Days - Day(Current Date) Days) - 60 Months), ISO) 
And CHAR((LAST_DAY(Current Date - 13 Months)), ISO) """
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
