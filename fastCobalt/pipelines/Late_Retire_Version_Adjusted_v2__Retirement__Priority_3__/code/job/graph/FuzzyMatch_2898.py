from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def FuzzyMatch_2898(spark: SparkSession, in0: DataFrame) -> DataFrame:
    
    from prophecy.utils.transpiler.dataframe_fcns import fuzzyDedup
    out = fuzzyDedup(in0, "ADDR_COMPLETE_APT_FIXED", 75.0, spark)

    return out0
