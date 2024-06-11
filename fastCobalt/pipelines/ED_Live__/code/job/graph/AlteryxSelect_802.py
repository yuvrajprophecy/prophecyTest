from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_802(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Polypharmacy"), 
        col("`Mental Health Claims`").alias("Mental Health Claims"), 
        col("Population"), 
        col("`Injury Related ED`").alias("Injury Related ED"), 
        col("`Drug Related ED`").alias("Drug Related ED"), 
        col("Relationship"), 
        col("`Service Area`").alias("Service Area"), 
        col("`ED Visited`").alias("ED Visited"), 
        col("`Group ID`").alias("Group ID"), 
        col("`Index Month`").alias("Index Month"), 
        col("`Total Claim Spend`").alias("Total Claim Spend"), 
        col("`Household ED Visit Alw Amt`").alias("Household ED Visit Alw Amt"), 
        col("PR"), 
        col("`7-12 Month ED Visit History`").alias("7-12 Month ED Visit History"), 
        col("Claims"), 
        col("`Members in Household`").alias("Members in Household"), 
        col("`Non Emergent Percentage`").alias("Non Emergent Percentage"), 
        col("IP"), 
        col("Right_CountDistinct_Week"), 
        col("`Member Age`").alias("Member Age"), 
        col("`Household ED Visits`").alias("Household ED Visits"), 
        col("Target"), 
        col("ZIP"), 
        col("`Diag Codes`").alias("Diag Codes"), 
        col("OP"), 
        col("`Alcohol Related ED`").alias("Alcohol Related ED"), 
        col("`Max Claim Line Spend`").alias("Max Claim Line Spend"), 
        col("`ED Visits`").alias("ED Visits"), 
        col("MBR_INDV_BE_KEY"), 
        col("`SPIRA Elligible`").alias("SPIRA Elligible"), 
        col("`Coverage Area`").alias("Coverage Area"), 
        col("`PCMH Attributed`").alias("PCMH Attributed"), 
        col("`CCI Score`").alias("CCI Score"), 
        col("`Hospital Stay Days`").alias("Hospital Stay Days"), 
        col("`Drug Classes`").alias("Drug Classes"), 
        col("Race"), 
        col("Industry"), 
        col("Gender"), 
        col("CountDistinct_Week"), 
        col("`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt"), 
        col("`ED Visit Alw Amt`").alias("ED Visit Alw Amt"), 
        col("`Distance to Closest ED`").alias("Distance to Closest ED"), 
        col("`Drug Counts`").alias("Drug Counts"), 
        col("`Historic Non Emergent Visit`").alias("Historic Non Emergent Visit"), 
        col("`Medical Home`").alias("Medical Home"), 
        col("Last_YMD"), 
        col("`Medical Home (str)`").alias("Medical Home (str)"), 
        col("Right_MBR_INDV_BE_KEY"), 
        col("`Household ED Visit Tot Alw Amt`").alias("Household ED Visit Tot Alw Amt"), 
        col("Last_Week"), 
        col("`New Drugs`").alias("New Drugs"), 
        col("`Psych Related ED`").alias("Psych Related ED"), 
        col("`PCP Attributed`").alias("PCP Attributed")
    )
