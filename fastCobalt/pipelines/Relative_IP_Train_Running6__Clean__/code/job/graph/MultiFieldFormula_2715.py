from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_2715(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(col("Allergy_Immunotherapy").isNull(), lit(0))\
          .otherwise(col("Allergy_Immunotherapy"))\
          .alias("Allergy_Immunotherapy"), 
        when(col("Allergy_Testing").isNull(), lit(0)).otherwise(col("Allergy_Testing")).alias("Allergy_Testing"), 
        when(col("Anesthesia").isNull(), lit(0)).otherwise(col("Anesthesia")).alias("Anesthesia"), 
        when(col("Cardiovascular___Cardiac_Catheterization___Injection_Procedures").isNull(), lit(0))\
          .otherwise(col("Cardiovascular___Cardiac_Catheterization___Injection_Procedures"))\
          .alias("Cardiovascular___Cardiac_Catheterization___Injection_Procedures"), 
        when(col("Cardiovascular___Diagnostic").isNull(), lit(0))\
          .otherwise(col("Cardiovascular___Diagnostic"))\
          .alias("Cardiovascular___Diagnostic"), 
        when(col("Cardiovascular___Noninvasive_Physiologic_Studies_and_Procedures").isNull(), lit(0))\
          .otherwise(col("Cardiovascular___Noninvasive_Physiologic_Studies_and_Procedures"))\
          .alias("Cardiovascular___Noninvasive_Physiologic_Studies_and_Procedures"), 
        when(col("Cardiovascular___Other_Procedures").isNull(), lit(0))\
          .otherwise(col("Cardiovascular___Other_Procedures"))\
          .alias("Cardiovascular___Other_Procedures"), 
        when(col("Chiropractor___Chiropractic_Manipulative_Treatment").isNull(), lit(0))\
          .otherwise(col("Chiropractor___Chiropractic_Manipulative_Treatment"))\
          .alias("Chiropractor___Chiropractic_Manipulative_Treatment"), 
        when(col("Consults").isNull(), lit(0)).otherwise(col("Consults")).alias("Consults"), 
        when(col("DME").isNull(), lit(0)).otherwise(col("DME")).alias("DME"), 
        when(col("ER_Visits_and_Observation_Care").isNull(), lit(0))\
          .otherwise(col("ER_Visits_and_Observation_Care"))\
          .alias("ER_Visits_and_Observation_Care"), 
        when(col("Glasses_Contacts").isNull(), lit(0)).otherwise(col("Glasses_Contacts")).alias("Glasses_Contacts"), 
        when(col("Hearing_Speech_Exams").isNull(), lit(0))\
          .otherwise(col("Hearing_Speech_Exams"))\
          .alias("Hearing_Speech_Exams"), 
        when(col("Immunizations").isNull(), lit(0)).otherwise(col("Immunizations")).alias("Immunizations"), 
        when(col("IP_Visits___Critical_Care_Services").isNull(), lit(0))\
          .otherwise(col("IP_Visits___Critical_Care_Services"))\
          .alias("IP_Visits___Critical_Care_Services"), 
        when(col("IP_Visits___Hospital_Visits").isNull(), lit(0))\
          .otherwise(col("IP_Visits___Hospital_Visits"))\
          .alias("IP_Visits___Hospital_Visits"), 
        when(col("IP_Visits___Neonatal_Visits").isNull(), lit(0))\
          .otherwise(col("IP_Visits___Neonatal_Visits"))\
          .alias("IP_Visits___Neonatal_Visits"), 
        when(col("IP_Visits___Newborn_Care").isNull(), lit(0))\
          .otherwise(col("IP_Visits___Newborn_Care"))\
          .alias("IP_Visits___Newborn_Care"), 
        when(col("IP_Visits___Nursing_Facility_Services").isNull(), lit(0))\
          .otherwise(col("IP_Visits___Nursing_Facility_Services"))\
          .alias("IP_Visits___Nursing_Facility_Services"), 
        when(col("Maternity_Deliveries").isNull(), lit(0))\
          .otherwise(col("Maternity_Deliveries"))\
          .alias("Maternity_Deliveries"), 
        when(col("Maternity_Other").isNull(), lit(0)).otherwise(col("Maternity_Other")).alias("Maternity_Other"), 
        when(col("Medical___Diagnostic___Biofeedback").isNull(), lit(0))\
          .otherwise(col("Medical___Diagnostic___Biofeedback"))\
          .alias("Medical___Diagnostic___Biofeedback"), 
        when(col("Medical___Diagnostic___Central_Nervous_System").isNull(), lit(0))\
          .otherwise(col("Medical___Diagnostic___Central_Nervous_System"))\
          .alias("Medical___Diagnostic___Central_Nervous_System"), 
        when(col("Medical___Diagnostic___ENT").isNull(), lit(0))\
          .otherwise(col("Medical___Diagnostic___ENT"))\
          .alias("Medical___Diagnostic___ENT"), 
        when(col("Medical___Diagnostic___Gastroenterology").isNull(), lit(0))\
          .otherwise(col("Medical___Diagnostic___Gastroenterology"))\
          .alias("Medical___Diagnostic___Gastroenterology"), 
        when(col("Medical___Diagnostic___Neurology_and_Neuromuscular_Procedures").isNull(), lit(0))\
          .otherwise(col("Medical___Diagnostic___Neurology_and_Neuromuscular_Procedures"))\
          .alias("Medical___Diagnostic___Neurology_and_Neuromuscular_Procedures"), 
        when(col("Medical___Diagnostic___Vascular").isNull(), lit(0))\
          .otherwise(col("Medical___Diagnostic___Vascular"))\
          .alias("Medical___Diagnostic___Vascular"), 
        when(col("Miscellaneous_Medical").isNull(), lit(0))\
          .otherwise(col("Miscellaneous_Medical"))\
          .alias("Miscellaneous_Medical"), 
        when(col("Non_Prescription_Drugs").isNull(), lit(0))\
          .otherwise(col("Non_Prescription_Drugs"))\
          .alias("Non_Prescription_Drugs"), 
        when(col("Non_Standard_Benefit").isNull(), lit(0))\
          .otherwise(col("Non_Standard_Benefit"))\
          .alias("Non_Standard_Benefit"), 
        when(col("Office_Home_Visits___Care_Plan_Oversight_Services").isNull(), lit(0))\
          .otherwise(col("Office_Home_Visits___Care_Plan_Oversight_Services"))\
          .alias("Office_Home_Visits___Care_Plan_Oversight_Services"), 
        when(col("Office_Home_Visits___Office_Visits").isNull(), lit(0))\
          .otherwise(col("Office_Home_Visits___Office_Visits"))\
          .alias("Office_Home_Visits___Office_Visits"), 
        when(col("Office_Home_Visits___Prolonged_Services").isNull(), lit(0))\
          .otherwise(col("Office_Home_Visits___Prolonged_Services"))\
          .alias("Office_Home_Visits___Prolonged_Services"), 
        when(col("Office_Home_Visits___Rest_Home").isNull(), lit(0))\
          .otherwise(col("Office_Home_Visits___Rest_Home"))\
          .alias("Office_Home_Visits___Rest_Home"), 
        when(col("Office_Home_Visits___Telephonic_and_Email").isNull(), lit(0))\
          .otherwise(col("Office_Home_Visits___Telephonic_and_Email"))\
          .alias("Office_Home_Visits___Telephonic_and_Email"), 
        when(col("Pathology_and_Lab").isNull(), lit(0)).otherwise(col("Pathology_and_Lab")).alias("Pathology_and_Lab"), 
        when(col("PDN_HH").isNull(), lit(0)).otherwise(col("PDN_HH")).alias("PDN_HH"), 
        when(col("Physical_Exams").isNull(), lit(0)).otherwise(col("Physical_Exams")).alias("Physical_Exams"), 
        when(col("Physical_Therapy").isNull(), lit(0)).otherwise(col("Physical_Therapy")).alias("Physical_Therapy"), 
        when(col("Psychiatric_Alcohol_Drug_Abuse").isNull(), lit(0))\
          .otherwise(col("Psychiatric_Alcohol_Drug_Abuse"))\
          .alias("Psychiatric_Alcohol_Drug_Abuse"), 
        when(col("Radiology___CT_MRI_PET").isNull(), lit(0))\
          .otherwise(col("Radiology___CT_MRI_PET"))\
          .alias("Radiology___CT_MRI_PET"), 
        when(col("Radiology___Diagnostic___Bone_Joint_Studies").isNull(), lit(0))\
          .otherwise(col("Radiology___Diagnostic___Bone_Joint_Studies"))\
          .alias("Radiology___Diagnostic___Bone_Joint_Studies"), 
        when(col("Radiology___Diagnostic___Diagnostic_Imaging").isNull(), lit(0))\
          .otherwise(col("Radiology___Diagnostic___Diagnostic_Imaging"))\
          .alias("Radiology___Diagnostic___Diagnostic_Imaging"), 
        when(col("Radiology___Diagnostic___Other_Procedures").isNull(), lit(0))\
          .otherwise(col("Radiology___Diagnostic___Other_Procedures"))\
          .alias("Radiology___Diagnostic___Other_Procedures"), 
        when(col("Radiology___Nuclear").isNull(), lit(0))\
          .otherwise(col("Radiology___Nuclear"))\
          .alias("Radiology___Nuclear"), 
        when(col("Radiology___Therapeutic").isNull(), lit(0))\
          .otherwise(col("Radiology___Therapeutic"))\
          .alias("Radiology___Therapeutic"), 
        when(col("Surgery___Auditory_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Auditory_System"))\
          .alias("Surgery___Auditory_System"), 
        when(col("Surgery___Cardiovascular_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Cardiovascular_System"))\
          .alias("Surgery___Cardiovascular_System"), 
        when(col("Surgery___Digestive_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Digestive_System"))\
          .alias("Surgery___Digestive_System"), 
        when(col("Surgery___Endocrine_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Endocrine_System"))\
          .alias("Surgery___Endocrine_System"), 
        when(col("Surgery___Eye_and_Ocular_Adnexa").isNull(), lit(0))\
          .otherwise(col("Surgery___Eye_and_Ocular_Adnexa"))\
          .alias("Surgery___Eye_and_Ocular_Adnexa"), 
        when(col("Surgery___Female_Genital_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Female_Genital_System"))\
          .alias("Surgery___Female_Genital_System"), 
        when(col("Surgery___General___Needle_Aspiration").isNull(), lit(0))\
          .otherwise(col("Surgery___General___Needle_Aspiration"))\
          .alias("Surgery___General___Needle_Aspiration"), 
        when(col("Surgery___Hemic_and_Lymphatic").isNull(), lit(0))\
          .otherwise(col("Surgery___Hemic_and_Lymphatic"))\
          .alias("Surgery___Hemic_and_Lymphatic"), 
        when(col("Surgery___Integumentary_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Integumentary_System"))\
          .alias("Surgery___Integumentary_System"), 
        when(col("Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_Lesions").isNull(), lit(0))\
          .otherwise(col("Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_Lesions"))\
          .alias("Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_Lesions"), 
        when(col("Surgery___Integumentary_System___Destruction__Malignant_Lesions").isNull(), lit(0))\
          .otherwise(col("Surgery___Integumentary_System___Destruction__Malignant_Lesions"))\
          .alias("Surgery___Integumentary_System___Destruction__Malignant_Lesions"), 
        when(col("Surgery___Integumentary_System___Excision_of_Malignant_Lesions").isNull(), lit(0))\
          .otherwise(col("Surgery___Integumentary_System___Excision_of_Malignant_Lesions"))\
          .alias("Surgery___Integumentary_System___Excision_of_Malignant_Lesions"), 
        when(col("Surgery___Male_Genital_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Male_Genital_System"))\
          .alias("Surgery___Male_Genital_System"), 
        when(col("Surgery___Musculoskeletal").isNull(), lit(0))\
          .otherwise(col("Surgery___Musculoskeletal"))\
          .alias("Surgery___Musculoskeletal"), 
        when(col("Surgery___Nervous_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Nervous_System"))\
          .alias("Surgery___Nervous_System"), 
        when(col("Surgery___Respiratory_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Respiratory_System"))\
          .alias("Surgery___Respiratory_System"), 
        when(col("Surgery___Urinary_System").isNull(), lit(0))\
          .otherwise(col("Surgery___Urinary_System"))\
          .alias("Surgery___Urinary_System"), 
        when(col("Urgent_Care_Visits").isNull(), lit(0))\
          .otherwise(col("Urgent_Care_Visits"))\
          .alias("Urgent_Care_Visits"), 
        when(col("Vision_Exams").isNull(), lit(0)).otherwise(col("Vision_Exams")).alias("Vision_Exams"), 
        when(col("Well_Baby_Exams").isNull(), lit(0)).otherwise(col("Well_Baby_Exams")).alias("Well_Baby_Exams"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY")
    )
