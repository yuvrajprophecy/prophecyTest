from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from p4.config.ConfigStore import *
from p4.udfs.UDFs import *
from prophecy.utils import *
from p4.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/p4")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/p4")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
