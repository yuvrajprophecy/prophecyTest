from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_S(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_S}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_S}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_S}")\
        .option(
          "query",
          """Select MBR.MBR_SK, 
 
\tcase 
\t\t\twhen MBR.MBR_BRTH_DT_SK <> 'UNK' and MBR.MBR_BRTH_DT_SK is not NULL and MBR.MBR_BRTH_DT_SK <> 'NA' then (Days(Current Date) - Days(DATE(MBR.MBR_BRTH_DT_SK))) / 365 
\t\t\telse 9999 
\t\tend as MEMBER_AGE,
Left(YR_MO.FIRST_DT_OF_MO,7) as YEARMONTH,

Case 
When MBR.MBR_BRTH_DT_SK <> 'UNK' 
And MBR.MBR_BRTH_DT_SK Is Not Null 
And MBR.MBR_BRTH_DT_SK <> 'NA' 
Then CAST((Days(Current Date) - Days(DATE(MBR.MBR_BRTH_DT_SK))) / 365 AS DECIMAL (7,2))
Else 9999 
End As MEMBER_AGE, 

MBR.MBR_GNDR_CD, 
Sum(RCST.MBR_CT) As TOT_MONTHS, 
MBR.MBR_INDV_BE_KEY 
From PROD.MBR_D MBR 
Inner Join PROD.MBR_RCST_CT_F RCST On MBR.MBR_SK = RCST.MBR_SK 
Inner Join PROD.YR_MO_D YR_MO On RCST.ACTVTY_YR_MO_SK = YR_MO.YR_MO_SK 
Where RCST.PROD_BILL_CMPNT_COV_CAT_CD = 'MED' 
And RCST.PROD_SH_NM In ('PCB', 'BCARE', 'BLUE-SELECT', 'BLUESELECT+', 'BLUE-ACCESS', 'PC', 'BMADVH', 'BMADVP', 'HP') 
And YR_MO.FIRST_DT_OF_MO Between CHAR(((Current Date + 1 Days - Day(Current Date) Days) - 49 Months), ISO) 
And CHAR((LAST_DAY(Current Date - 13 Months)), ISO) 

Group By MBR.MBR_SK, MBR.MBR_GNDR_CD, MBR.MBR_INDV_BE_KEY, MBR.MBR_BRTH_DT_SK, Left(YR_MO.FIRST_DT_OF_MO,7) """
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
