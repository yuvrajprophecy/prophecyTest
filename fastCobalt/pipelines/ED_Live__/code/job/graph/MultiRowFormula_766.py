from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_766(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("id", monotonically_increasing_id())\
        .withColumn("ED Visits_lag52", lag(col("`ED Visits`"), 52).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag51", lag(col("`ED Visits`"), 51).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag50", lag(col("`ED Visits`"), 50).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag49", lag(col("`ED Visits`"), 49).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag48", lag(col("`ED Visits`"), 48).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag47", lag(col("`ED Visits`"), 47).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag46", lag(col("`ED Visits`"), 46).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag45", lag(col("`ED Visits`"), 45).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag44", lag(col("`ED Visits`"), 44).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag43", lag(col("`ED Visits`"), 43).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag42", lag(col("`ED Visits`"), 42).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag41", lag(col("`ED Visits`"), 41).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag40", lag(col("`ED Visits`"), 40).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag39", lag(col("`ED Visits`"), 39).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag38", lag(col("`ED Visits`"), 38).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag37", lag(col("`ED Visits`"), 37).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag36", lag(col("`ED Visits`"), 36).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag35", lag(col("`ED Visits`"), 35).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag34", lag(col("`ED Visits`"), 34).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag33", lag(col("`ED Visits`"), 33).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag32", lag(col("`ED Visits`"), 32).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag31", lag(col("`ED Visits`"), 31).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag30", lag(col("`ED Visits`"), 30).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag29", lag(col("`ED Visits`"), 29).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag28", lag(col("`ED Visits`"), 28).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("ED Visits_lag27", lag(col("`ED Visits`"), 27).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn(
          "History of ED Visits",
          when(
              (
                (((((((((((((((((((((((((col("`ED Visits_lag52`") + col("`ED Visits_lag51`")) + col("`ED Visits_lag50`")) + col("`ED Visits_lag49`")) + col("`ED Visits_lag48`")) + col("`ED Visits_lag47`")) + col("`ED Visits_lag46`")) + col("`ED Visits_lag45`")) + col("`ED Visits_lag44`")) + col("`ED Visits_lag43`")) + col("`ED Visits_lag42`")) + col("`ED Visits_lag41`")) + col("`ED Visits_lag40`")) + col("`ED Visits_lag39`")) + col("`ED Visits_lag38`")) + col("`ED Visits_lag37`")) + col("`ED Visits_lag36`")) + col("`ED Visits_lag35`")) + col("`ED Visits_lag34`")) + col("`ED Visits_lag33`")) + col("`ED Visits_lag32`")) + col("`ED Visits_lag31`")) + col("`ED Visits_lag30`")) + col("`ED Visits_lag29`")) + col("`ED Visits_lag28`")) + col("`ED Visits_lag27`")).cast(IntegerType())
                > lit(0)
              ), 
              lit(1)
            )\
            .otherwise(lit(0))
        )\
        .drop("ED Visits_lag52")\
        .drop("ED Visits_lag51")\
        .drop("ED Visits_lag50")\
        .drop("ED Visits_lag49")\
        .drop("ED Visits_lag48")\
        .drop("ED Visits_lag47")\
        .drop("ED Visits_lag46")\
        .drop("ED Visits_lag45")\
        .drop("ED Visits_lag44")\
        .drop("ED Visits_lag43")\
        .drop("ED Visits_lag42")\
        .drop("ED Visits_lag41")\
        .drop("ED Visits_lag40")\
        .drop("ED Visits_lag39")\
        .drop("ED Visits_lag38")\
        .drop("ED Visits_lag37")\
        .drop("ED Visits_lag36")\
        .drop("ED Visits_lag35")\
        .drop("ED Visits_lag34")\
        .drop("ED Visits_lag33")\
        .drop("ED Visits_lag32")\
        .drop("ED Visits_lag31")\
        .drop("ED Visits_lag30")\
        .drop("ED Visits_lag29")\
        .drop("ED Visits_lag28")\
        .drop("ED Visits_lag27")\
        .drop("id")
