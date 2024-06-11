from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_2707(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("YEARMONTH"))

    return df1.agg(
        sum(col("Surgery___General___Needle_Aspiration")).alias("Sum_Surgery___General___Needle_Aspiration"), 
        sum(col("DME")).alias("Sum_DME"), 
        sum(col("Surgery___Respiratory_System")).alias("Sum_Surgery___Respiratory_System"), 
        sum(col("Office_Home_Visits___Care_Plan_Oversight_Services"))\
          .alias("Sum_Office_Home_Visits___Care_Plan_Oversight_Services"), 
        sum(col("Surgery___Integumentary_System")).alias("Sum_Surgery___Integumentary_System"), 
        sum(col("Radiology___Therapeutic")).alias("Sum_Radiology___Therapeutic"), 
        sum(col("Anesthesia")).alias("Sum_Anesthesia"), 
        sum(col("Maternity_Deliveries")).alias("Sum_Maternity_Deliveries"), 
        sum(col("Non_Standard_Benefit")).alias("Sum_Non_Standard_Benefit"), 
        sum(col("Glasses_Contacts")).alias("Sum_Glasses_Contacts"), 
        sum(col("Radiology___Diagnostic___Diagnostic_Imaging")).alias("Sum_Radiology___Diagnostic___Diagnostic_Imaging"), 
        sum(col("IP_Visits___Neonatal_Visits")).alias("Sum_IP_Visits___Neonatal_Visits"), 
        sum(col("Pathology_and_Lab")).alias("Sum_Pathology_and_Lab"), 
        sum(col("Surgery___Female_Genital_System")).alias("Sum_Surgery___Female_Genital_System"), 
        sum(col("Medical___Diagnostic___Gastroenterology")).alias("Sum_Medical___Diagnostic___Gastroenterology"), 
        sum(col("Surgery___Digestive_System")).alias("Sum_Surgery___Digestive_System"), 
        sum(col("IP_Visits___Critical_Care_Services")).alias("Sum_IP_Visits___Critical_Care_Services"), 
        sum(col("IP_Visits___Newborn_Care")).alias("Sum_IP_Visits___Newborn_Care"), 
        sum(col("Physical_Therapy")).alias("Sum_Physical_Therapy"), 
        sum(col("Cardiovascular___Diagnostic")).alias("Sum_Cardiovascular___Diagnostic"), 
        sum(col("Maternity_Other")).alias("Sum_Maternity_Other"), 
        sum(col("Surgery___Hemic_and_Lymphatic")).alias("Sum_Surgery___Hemic_and_Lymphatic"), 
        sum(col("IP_Visits___Nursing_Facility_Services")).alias("Sum_IP_Visits___Nursing_Facility_Services"), 
        sum(col("Surgery___Cardiovascular_System")).alias("Sum_Surgery___Cardiovascular_System"), 
        sum(col("Radiology___Diagnostic___Other_Procedures")).alias("Sum_Radiology___Diagnostic___Other_Procedures"), 
        sum(col("Radiology___Diagnostic___Bone_Joint_Studies")).alias("Sum_Radiology___Diagnostic___Bone_Joint_Studies"), 
        sum(col("Allergy_Testing")).alias("Sum_Allergy_Testing"), 
        sum(col("Surgery___Musculoskeletal")).alias("Sum_Surgery___Musculoskeletal"), 
        sum(col("Medical___Diagnostic___Neurology_and_Neuromuscular_Procedures"))\
          .alias("Sum_Medical___Diagnostic___Neurology_and_Neuromuscular_Procedures"), 
        sum(col("Office_Home_Visits___Office_Visits")).alias("Sum_Office_Home_Visits___Office_Visits"), 
        sum(col("Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_Lesions"))\
          .alias("Sum_Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_Lesions"), 
        sum(col("Consults")).alias("Sum_Consults"), 
        sum(col("Cardiovascular___Other_Procedures")).alias("Sum_Cardiovascular___Other_Procedures"), 
        sum(col("Hearing_Speech_Exams")).alias("Sum_Hearing_Speech_Exams"), 
        sum(col("Immunizations")).alias("Sum_Immunizations"), 
        sum(col("Surgery___Auditory_System")).alias("Sum_Surgery___Auditory_System"), 
        sum(col("IP_Visits___Hospital_Visits")).alias("Sum_IP_Visits___Hospital_Visits"), 
        sum(col("Well_Baby_Exams")).alias("Sum_Well_Baby_Exams"), 
        sum(col("Surgery___Endocrine_System")).alias("Sum_Surgery___Endocrine_System"), 
        sum(col("Miscellaneous_Medical")).alias("Sum_Miscellaneous_Medical"), 
        sum(col("Allergy_Immunotherapy")).alias("Sum_Allergy_Immunotherapy"), 
        sum(col("Surgery___Eye_and_Ocular_Adnexa")).alias("Sum_Surgery___Eye_and_Ocular_Adnexa"), 
        sum(col("Non_Prescription_Drugs")).alias("Sum_Non_Prescription_Drugs"), 
        sum(col("Medical___Diagnostic___Central_Nervous_System"))\
          .alias("Sum_Medical___Diagnostic___Central_Nervous_System"), 
        sum(col("Surgery___Nervous_System")).alias("Sum_Surgery___Nervous_System"), 
        sum(col("Physical_Exams")).alias("Sum_Physical_Exams"), 
        sum(col("ER_Visits_and_Observation_Care")).alias("ER_Visits_and_Observation_Care"), 
        sum(col("Cardiovascular___Noninvasive_Physiologic_Studies_and_Procedures"))\
          .alias("Sum_Cardiovascular___Noninvasive_Physiologic_Studies_and_Procedures"), 
        sum(col("Surgery___Male_Genital_System")).alias("Sum_Surgery___Male_Genital_System"), 
        sum(col("Surgery___Integumentary_System___Excision_of_Malignant_Lesions"))\
          .alias("Sum_Surgery___Integumentary_System___Excision_of_Malignant_Lesions"), 
        sum(col("Medical___Diagnostic___Biofeedback")).alias("Sum_Medical___Diagnostic___Biofeedback"), 
        sum(col("Office_Home_Visits___Telephonic_and_Email")).alias("Sum_Office_Home_Visits___Telephonic_and_Email"), 
        sum(col("Vision_Exams")).alias("Sum_Vision_Exams"), 
        sum(col("Radiology___Nuclear")).alias("Sum_Radiology___Nuclear"), 
        sum(col("Radiology___CT_MRI_PET")).alias("Sum_Radiology___CT_MRI_PET"), 
        sum(col("Cardiovascular___Cardiac_Catheterization___Injection_Procedures"))\
          .alias("Sum_Cardiovascular___Cardiac_Catheterization___Injection_Procedures"), 
        sum(col("PDN_HH")).alias("Sum_PDN_HH"), 
        sum(col("Office_Home_Visits___Prolonged_Services")).alias("Sum_Office_Home_Visits___Prolonged_Services"), 
        sum(col("Psychiatric_Alcohol_Drug_Abuse")).alias("Sum_Psychiatric_Alcohol_Drug_Abuse"), 
        sum(col("Medical___Diagnostic___Vascular")).alias("Sum_Medical___Diagnostic___Vascular"), 
        sum(col("Surgery___Urinary_System")).alias("Sum_Surgery___Urinary_System"), 
        sum(col("Office_Home_Visits___Rest_Home")).alias("Sum_Office_Home_Visits___Rest_Home"), 
        sum(col("Surgery___Integumentary_System___Destruction__Malignant_Lesions"))\
          .alias("Sum_Surgery___Integumentary_System___Destruction__Malignant_Lesions"), 
        sum(col("Chiropractor___Chiropractic_Manipulative_Treatment"))\
          .alias("Sum_Chiropractor___Chiropractic_Manipulative_Treatment"), 
        sum(col("Medical___Diagnostic___ENT")).alias("Sum_Medical___Diagnostic___ENT"), 
        sum(col("Urgent_Care_Visits")).alias("Sum_Urgent_Care_Visits")
    )
