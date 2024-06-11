from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def aka_alxaa2_Query_SEL_4098(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_aka_alxaa2_Query_SEL_4098}")\
        .option("user", f"{Config.username_aka_alxaa2_Query_SEL_4098}")\
        .option("password", f"{Config.password_aka_alxaa2_Query_SEL_4098}")\
        .option(
          "query",
          """SELECT 
\t--Keys
\tm.mbr_indv_be_key,
\tleft(c.CLM_SVC_STRT_DT_SK, 7) \"Service Date\", 
\tleft(c.clm_rcvd_dt_sk, 7) \"Received Date\",
\tc.DIAG_CD_1_SK,
\tc.NDC_SK,
\tcl.CLM_LN_PROC_CD_SK,
\tc.DIAG_CD_1_SK,
\tc.DIAG_CD_2_SK,
\tc.DIAG_CD_3_SK,
\tc.PROC_CD_1_SK,
\tc.PROC_CD_2_SK,
\tc.PROC_CD_3_SK,
\t--Claim Types
\tc.CLM_TYP_CD \"Claim Type lvl 1\",
\tc.CLM_SUBTYP_CD \"Claim Type lvl 2\",
\tcl.DNTL_CLM_LN_DNTL_CAT_CD \"Dental Category Code\",
\tc.DIAG_CD_CAT_CD \"Diagnosis Type\",
\tcl.clm_ln_pos_cd,
\t--Potential Filters
\tc.SRC_SYS_CD, 
\tc.FUND_CAT_CD,
\tc.SRC_SYS_CD,
\tc.CLM_FINL_DISP_CD,
\t--g.GRP_MKT_SIZE_CAT_CD,
\t--Drug Info
\tc.DRUG_CLM_TIER_CD,
\tc.DRUG_CLM_SPEC_DRUG_IN \"SpecialtyIndicator\",
\tc.DRUG_CLM_GNRC_DRUG_IN \"GenericIndicator\",
\tc.DRUG_CLM_MAIL_ORDER_IN \"MailOrder\",
\t--Payment Info
  (c.CLM_LN_TOT_PAYBL_AMT - c.CLM_LN_TOT_PAYBL_TO_PROV_AMT) \"MemberPay\",
\tc.DRUG_CLM_INGR_CST_ALW_AMT \"IngredientCost\",
\tc.DRUG_CLM_UCR_AMT \"RetailCost\"

FROM 
\tPROD.CLM_F c 
\t--JOIN 
\t--\tPROD.CLM_F2 c2 on c2.CLM_SK = c.CLM_SK
\tinner JOIN 
\t\tPROD.CLM_LN_F cl ON cl.CLM_SK = c.CLM_SK
\t--JOIN 
\t--\tPROD.GRP_D g ON g.GRP_SK = c.GRP_SK
\tinner join
\t\tprod.mbr_d m on m.mbr_sk = c.mbr_sk
\t--left join 
\t--\tNDC_D ND on ND.NDC_SK = c.NDC_SK
\t--left join 
\t--\tDRUG_CLM_PRICE_F DCPF on DCPF.CLM_ID = C.CLM_ID

WHERE   
\tc.CLM_CAT_CD='STD' 
\t--AND c.CLM_FINL_DISP_CD='ACPTD' 
\tAND c.CLM_STTUS_CD in ('A02','A08','A09') 
--AND CLM_F.CLM_TYP_CD='MED' 
--AND CLM_F.SRC_SYS_CD in ('ESI','OPTUMRX' ,'FACETS')
\t--and cl.PROD_SH_NM in ('PCB', 'BCARE', 'BLUE-SELECT', 'BLUESELECT+', 'BLUE-ACCESS', 'PC', 'PCBEXTRNL') 
--AND GRP_D.GRP_MKT_SIZE_CAT_CD in ('LGGRP1', 'LGGRP2', 'LGGRP3', 'MDGRP1', 'MDGRP2', 'SMGRP2', 'SMGRP1', 'MCARE', 'MCAREDRPAY', 'MCAREGRP', 'ASO', 'DP') 
\tAND c.clm_svc_strt_dt_sk >= char(current date - 49 month, iso)
\tand c.clm_pd_dt_sk >= char(current date - 49 month, iso)
\tAND c.clm_svc_strt_dt_sk < char(current date, iso)
\tand c.clm_pd_dt_sk < char(current date, iso)
\tand c.prod_sh_nm not in ('BMADVH','BMADVP','CBLU65','DENTMA','MEDGAP','MEDSEL','THC65','TIP')
\tand M.MBR_BRTH_DT_SK <= CHAR(current date - 650 Months, ISO)"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
