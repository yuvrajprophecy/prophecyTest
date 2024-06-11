from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_S_2350(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_S_2350}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_S_2350}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_S_2350}")\
        .option(
          "query",
          """Select MBR.MBR_SK, CLM.CLM_SUBTYP_CD, 
Sum(CLM.CLM_CT) As CASES,
left(CLM.CLM_SVC_STRT_DT_SK, 7) as YEARMONTH 
From PROD.CLM_F As CLM 
Inner Join PROD.MBR_D As MBR On CLM.MBR_SK = MBR.MBR_SK 
Where CLM.CLM_SVC_STRT_DT_SK Between char(((Current Date + 1 Days - Day(Current Date) Days) - 60 Months), ISO) 
And CHAR((LAST_DAY(Current Date - 2 Months)), ISO) 
And CLM.SRC_SYS_CD In ('FACETS', 'ESI', 'MEDTRAK', 'CVS', 'OPTUMRX', 'SAVRX') 
And CLM.CLM_STTUS_CD In ('A02') 
And CLM.CLM_FINL_DISP_CD = 'ACPTD' 
And CLM.CLM_TYP_CD = 'MED' And CLM.CLM_CAT_CD = 'STD' 
And CLM.CLM_HOST_IN = 'N' 
And CLM.CLM_PD_DT_SK <= CHAR((LAST_DAY(Current Date - 1 Months)), ISO)
And CLM.CLM_SUBTYP_CD = 'IP'
Group By MBR.MBR_SK, CLM.CLM_SUBTYP_CD, left(CLM.CLM_SVC_STRT_DT_SK, 7)"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
