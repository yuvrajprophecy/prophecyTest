from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Transpose_618(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def transposeDataFrame(df: DataFrame, keyColumns, dataColumns):
        unpivoted_cols = (
            [df[col] for col in keyColumns]
            + [lit(col).alias("Name") for col in dataColumns]
            + [df[col].alias("Value") for col in dataColumns]
        )

        return df.select(*unpivoted_cols)

    out0 = transposeDataFrame(in0, [], ["1"])

    return out0
