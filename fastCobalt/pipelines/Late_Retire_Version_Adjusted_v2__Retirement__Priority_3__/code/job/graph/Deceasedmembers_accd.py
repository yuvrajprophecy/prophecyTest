from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Deceasedmembers_accd(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_Deceasedmembers_accd}")\
        .option("user", f"{Config.username_Deceasedmembers_accd}")\
        .option("password", f"{Config.password_Deceasedmembers_accd}")\
        .option("query", "`Deceased members_Final`")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
