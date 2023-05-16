from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from p1.config.ConfigStore import *
from p1.udfs.UDFs import *
from prophecy.utils import *
from p1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds1 = ds1(spark)
    df_Reformat_1 = Reformat_1(spark, df_ds1)
    df_sg2 = sg2(spark, Config.sg2, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/p2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/p2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
