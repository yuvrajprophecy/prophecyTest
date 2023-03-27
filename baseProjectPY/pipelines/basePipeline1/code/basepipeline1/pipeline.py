from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from basepipeline1.config.ConfigStore import *
from basepipeline1.udfs.UDFs import *
from prophecy.utils import *
from basepipeline1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_baseDS1 = baseDS1(spark)
    df_Reformat_1 = Reformat_1(spark, df_baseDS1)
    df_Subgraph_1 = Subgraph_1(spark, Config.Subgraph_1, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/basePipeline1")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/basePipeline1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
