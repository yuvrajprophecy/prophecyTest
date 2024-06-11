from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ClassifyProductTypes(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Marketing Restricted",
          when(
              array_contains(
                array(
                  lit("10023000"), 
                  lit("10023000"), 
                  lit("10030000"), 
                  lit("12763000"), 
                  lit("31585000"), 
                  lit("85000000"), 
                  lit("85000001"), 
                  lit("85000002"), 
                  lit("85000003"), 
                  lit("85000004"), 
                  lit("85000005"), 
                  lit("85000006"), 
                  lit("85000007"), 
                  lit("85000008"), 
                  lit("85000009"), 
                  lit("32478000"), 
                  lit("10037000"), 
                  lit("10034000"), 
                  lit("10025000"), 
                  lit("24166000"), 
                  lit("27350000")
                ), 
                col("GRP_ID").cast(StringType())
              ), 
              lit("Restricted")
            )\
            .otherwise(lit("Not Restricted"))
        )\
        .withColumn(
          "Product Category",
          when(
              array_contains(
                array(
                  lit("M016"), 
                  lit("M116"), 
                  lit("M419"), 
                  lit("K419"), 
                  lit("M519"), 
                  lit("K519"), 
                  lit("M019"), 
                  lit("K019"), 
                  lit("M119"), 
                  lit("K119"), 
                  lit("M424"), 
                  lit("K424"), 
                  lit("M524"), 
                  lit("K524"), 
                  lit("M024"), 
                  lit("K024"), 
                  lit("M124"), 
                  lit("K124")
                ), 
                col("FNCL_LOB_CD").cast(StringType())
              ), 
              lit("ACA")
            )\
            .when((col("EXPRNC_CAT_CD").cast(StringType()) == lit("FEP")), lit("FEP"))\
            .when(
              (
                array_contains(array(lit("BMADVP"), lit("BMADVH"), lit("MEDGAP"), lit("MEDSEL"), lit("TIP"), lit("CBLU65")), col("Product").cast(StringType()))
                | array_contains(array(lit("K011"), lit("M011")), col("FNCL_LOB_CD").cast(StringType()))
              ), 
              lit("Retire Prod")
            )\
            .when(
              array_contains(
                array(
                  lit("PCB"), 
                  lit("BCARE"), 
                  lit("BLUE-SELECT"), 
                  lit("BLUESELECT+"), 
                  lit("BLUE-ACCESS"), 
                  lit("PC"), 
                  lit("HP")
                ), 
                col("Product").cast(StringType())
              ), 
              lit("COMMERCIAL")
            )\
            .when((col("Product").contains(lit("DENT")) | col("Product").contains(lit("VISION"))), lit("DENT/VISION"))\
            .otherwise(lit("OTHER"))
        )\
        .withColumn("Retire", when((col("`Product Category`").cast(StringType()) == lit("Retire Prod")), lit(1)).otherwise(lit(0)))
