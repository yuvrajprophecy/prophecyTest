from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def P1_COBALTHS_NYUEDICD_384(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_P1_COBALTHS_NYUEDICD_384}")\
        .option("user", f"{Config.username_P1_COBALTHS_NYUEDICD_384}")\
        .option("password", f"{Config.password_P1_COBALTHS_NYUEDICD_384}")\
        .option("query", "Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.NYU ED ICD_10.txt")\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
