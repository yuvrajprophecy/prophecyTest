from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_4036(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((((col("HOST_MBR_IN").cast(StringType()) == lit("Y")) | array_contains(array(lit("ASO (Market Segment)"), lit("Large Group (Market Segment)"), lit("Large Group Student (Market Segment)"), lit("Minimum Premium (Market Segment)"), lit("Minimum Premium Pooling (Market Segment)"), lit("Medicare Advantage (Market Segment)"), lit("Other (Market Segment)"), lit("Small Group (Market Segment)"), lit("Small Group ACA (Market Segment)"), lit("IFP (Market Segment)"), lit("IFP ACA (Market Segment)"), lit("Medicare Supplement (Market Segment)")), col("FNCL_MKT_SEG_NM").cast(StringType()))) & (array_contains(array(lit("PCB"), lit("BCARE"), lit("BLUE-SELECT"), lit("BLUESELECT+"), lit("BLUE-ACCESS"), lit("PC"), lit("BMADVP"), lit("BMADVH"), lit("MEDGAP"), lit("MEDSEL"), lit("HP"), lit("CBLU65"), lit("TIP")), col("PROD_SH_NM").cast(StringType())) | array_contains(array(lit("M016"), lit("M116"), lit("M419"), lit("K419"), lit("M519"), lit("K519"), lit("M019"), lit("K019"), lit("M119"), lit("K119"), lit("M424"), lit("K424"), lit("M524"), lit("K524"), lit("M024"), lit("K024"), lit("M124"), lit("K124"), lit("M999")), col("FNCL_LOB_CD").cast(StringType())))) & array_contains(array(lit("ASO"), lit("RISK"), lit("MINP"), lit("CPLS"), lit("NA")), col("FUND_CAT_CD").cast(StringType())))
          & array_contains(
            array(lit("PPO"), lit("HMO"), lit("TRAD"), lit("HPN")), 
            col("PROD_SH_NM_DLVRY_METH_CD").cast(StringType())
          )
        )
    )
