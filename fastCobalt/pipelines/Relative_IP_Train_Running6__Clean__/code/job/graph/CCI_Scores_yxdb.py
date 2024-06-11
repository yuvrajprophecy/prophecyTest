from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CCI_Scores_yxdb(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_CCI_Scores_yxdb}")\
        .option("user", f"{Config.username_CCI_Scores_yxdb}")\
        .option("password", f"{Config.password_CCI_Scores_yxdb}")\
        .option("query", "O:\\library\\Alteryx\\Advanced Analytics\\Reference Workflows\\CCI\\CCI_Scores.yxdb")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
