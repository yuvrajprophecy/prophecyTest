from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_794(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when((col("Target").isNull() | (length(col("Target")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("Target"))\
          .alias("Target"), 
        when(
            (
              col("`Plan Total ED Visits`").isNull()
              | (length(col("`Plan Total ED Visits`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`Plan Total ED Visits`"))\
          .alias("Plan Total ED Visits"), 
        when((col("`ED Visits`").isNull() | (length(col("`ED Visits`")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("`ED Visits`"))\
          .alias("ED Visits"), 
        when(
            (
              col("`ED Visits from Members in Household (Not Member)`").isNull()
              | (length(col("`ED Visits from Members in Household (Not Member)`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`ED Visits from Members in Household (Not Member)`"))\
          .alias("ED Visits from Members in Household (Not Member)"), 
        when(
            (
              col("`Members in Household`").isNull()
              | (length(col("`Members in Household`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`Members in Household`"))\
          .alias("Members in Household"), 
        when((col("injury").isNull() | (length(col("injury")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("injury"))\
          .alias("injury"), 
        when((col("psych").isNull() | (length(col("psych")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("psych"))\
          .alias("psych"), 
        when((col("alcohol").isNull() | (length(col("alcohol")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("alcohol"))\
          .alias("alcohol"), 
        when((col("drug").isNull() | (length(col("drug")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("drug"))\
          .alias("drug"), 
        when(
            (
              col("`Household ED Visit Alw Amt`").isNull()
              | (length(col("`Household ED Visit Alw Amt`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`Household ED Visit Alw Amt`"))\
          .alias("Household ED Visit Alw Amt"), 
        when(
            (
              col("`Household ED Visit Tot Alw Amt`").isNull()
              | (length(col("`Household ED Visit Tot Alw Amt`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`Household ED Visit Tot Alw Amt`"))\
          .alias("Household ED Visit Tot Alw Amt"), 
        when(
            (col("`ED Visit Alw Amt`").isNull() | (length(col("`ED Visit Alw Amt`")).cast(IntegerType()) > lit(0))), 
            lit(0)
          )\
          .otherwise(col("`ED Visit Alw Amt`"))\
          .alias("ED Visit Alw Amt"), 
        when(
            (
              col("`ED Visit Tot Alw Amt`").isNull()
              | (length(col("`ED Visit Tot Alw Amt`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`ED Visit Tot Alw Amt`"))\
          .alias("ED Visit Tot Alw Amt"), 
        when((col("DRUG_CLASS_COUNT").isNull() | (length(col("DRUG_CLASS_COUNT")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("DRUG_CLASS_COUNT"))\
          .alias("DRUG_CLASS_COUNT"), 
        when((col("DRUG_COUNT").isNull() | (length(col("DRUG_COUNT")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("DRUG_COUNT"))\
          .alias("DRUG_COUNT"), 
        when((col("POLYPHARMACY_IN").isNull() | (length(col("POLYPHARMACY_IN")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("POLYPHARMACY_IN"))\
          .alias("POLYPHARMACY_IN"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("MBR_RELSHP_NM"), 
        col("MED_HOME_GRP_DESC"), 
        col("YMD"), 
        col("SUB_CNTGS_CNTY_CD"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("DistaneMiles"), 
        col("PCP_FLAG"), 
        col("GRP_DP_IN"), 
        col("SUB_MKTNG_METRO_RURAL_CD"), 
        col("GRP_NM"), 
        col("GRP_ID"), 
        col("`Non Emergent Binary`").alias("Non Emergent Binary"), 
        col("CCI_Score"), 
        col("MBR_DSBLTY_IN"), 
        col("PROD_CAT"), 
        col("Week"), 
        col("SPIRA_BNF_ID"), 
        col("PROV_NM"), 
        col("MBR_GNDR_CD"), 
        col("MBR_ENR_COBRA_IN"), 
        col("mbr_age"), 
        col("`ED Visited`").alias("ED Visited"), 
        col("`Non Emergent Likelihood`").alias("Non Emergent Likelihood"), 
        col("FEP_FLAG"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("Race"), 
        col("MED_HOME_ID"), 
        col("MBR_UNIQ_KEY"), 
        col("Diagnoses")
    )
