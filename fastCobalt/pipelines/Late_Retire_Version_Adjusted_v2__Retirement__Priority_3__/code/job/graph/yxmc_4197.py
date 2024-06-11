from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def yxmc_4197(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def clean_columns(df, columns_to_clean):
        cleaned_df = df

        for column in columns_to_clean:
            column_type = df.schema[column].dataType

            if column_type == "string":
                cleaned_df = cleaned_df.withColumn(column, when(isnull(col(column)), "").otherwise(col(column)))
            elif column_type == "integer":
                cleaned_df = cleaned_df.withColumn(column, when(isnull(col(column)), 0).otherwise(col(column)))

        return cleaned_df

    def cleanse_data_frame(df: DataFrame) -> DataFrame:
        cleansed_df = df

        # Iterate over each column
        for col_name in df.columns:
            transformed_col = col(col_name)
            # Apply cleansing operations based on the boolean flags
            transformed_col = trim(transformed_col)
            # Replace the original column with the transformed column
            cleansed_df = cleansed_df.withColumn(col_name, transformed_col)

        return cleansed_df

    out2 = in0
    out1 = clean_columns(
        out2,
        ["MBR_INDV_BE_KEY", "YMD", "Target Forecasted", "YEARMONTH", "AGE", "MBR_DSBLTY_IN", "GRP_ID",
         "MBR_ENR_COBRA_IN", "PRNT_GRP_SIC_NACIS_CD", "GRP_MKT_SIZE_CAT_NM", "GRP_ZIP_CD_5", "GRP_TOT_EMPL_CT",
         "PROD_SH_NM", "FNCL_MKT_SEG_NM", "FUND_CAT_CD", "PROD_SH_NM_DLVRY_METH_CD", "HOST_MBR_IN",
         "FNCL_LOB_CD", "EXPRNC_CAT_CD", "CLS_PLN_DESC", "MBR_GNDR_CD", "SPIRA_BNF_ID", "MBR_HOME_ADDR_ST_CD",
         "MBR_HOME_ADDR_ZIP_CD_5", "SUB_MBR_SK", "MBR_RELSHP_NM", "PCP_FLAG", "Product Category", "Avg_BCARE",
         "Avg_BLUE_ACCESS", "Avg_BLUE_SELECT", "Avg_BLUESELECT_", "Avg_HPEXTRNL", "Avg_PC", "Avg_PCB",
         "Avg_PCBEXTRNL", "Retired With Blue", "Members_on_Policy", "Spouse Age", "Min_Dependent Age",
         "Male Percentage", "Female Percentage", "Average Age Males", "Average Age Females", "SUM_CCI",
         "Member Months", "Group Months", "Drug Tier 1", "Drug Tier 2", "Drug Tier 3", "Distinct NDC Codes",
         "Distinct Diagnosis Types", "Specialty Drug Counts", "Generic Drug Counts", "Mail Order Drug Counts",
         "Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC", "Vision EYE",
         "Vision FACTORS_INFLUENCING_HEALTH_STATUS", "Vision Member Pay", "RX Member Pay",
         "Medical ALCOHOL_DRUG_USE_AND_DISORDERS", "Medical BLOOD_AND_BLOOD_FORMING_ORGANS", "Medical BURNS",
         "Medical CIRCULATORY_SYSTEM", "Medical DIGESTIVE_SYSTEM", "Medical EAR__NOSE__AND_THROAT",
         "Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC", "Medical EYE",
         "Medical FACTORS_INFLUENCING_HEALTH_STATUS", "Medical FEMALE_REPRODUCTIVE_SYSTEM",
         "Medical HEPATOBILARY_SYSTEM", "Medical INFECTIOUS_AND_PARASITIC_DISEASES",
         "Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS", "Medical KIDNEY_AND_URINARY_TRACT",
         "Medical MALE_REPRODUCTIVE_SYSTEM", "Medical MENTAL_ILLNESS", "Medical MUSCULOSKETAL_SYSTEM",
         "Medical MYELOPROLIFERATIVE_AND_NEOPLASMS", "Medical NERVOUS_SYSTEM",
         "Medical NEWBORNS_AND_OTHER_NEONATES", "Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM",
         "Medical RESPIRATORY_SYSTEM", "Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST", "Med Member Pay",
         "Medical AIDS_HIV", "Dental ALCOHOL_DRUG_USE_AND_DISORDERS", "Dental CIRCULATORY_SYSTEM",
         "Dental EAR__NOSE__AND_THROAT", "Dental FACTORS_INFLUENCING_HEALTH_STATUS",
         "Dental MALE_REPRODUCTIVE_SYSTEM", "Dental MENTAL_ILLNESS", "Dental RESPIRATORY_SYSTEM",
         "Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST", "Dental Dental Member Pay", "Emergency_Room",
         "Inpatient", "Outpatient", "Number of Winterizing Procedures", "Dental Member Pay",
         "Depression Related Claims", "High Deductible Ind"]
    )
    out0 = cleanse_data_frame(out1)

    return out0
