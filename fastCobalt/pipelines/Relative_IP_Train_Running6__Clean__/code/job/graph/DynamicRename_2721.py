from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DynamicRename_2721(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("YEARMONTH"), 
        col("Sum_Surgery___Integumentary_System___Excision_of_Malignant_Lesions")\
          .alias("Surgery___Integumentary_System___Excision_of_Malignant_LesionsRunning3"), 
        col("Sum_Physical_Therapy").alias("Physical_TherapyRunning3"), 
        col("Sum_Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_Lesions")\
          .alias("Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_LesionsRunning3"), 
        col("Sum_Radiology___Nuclear").alias("Radiology___NuclearRunning3"), 
        col("Sum_Surgery___Respiratory_System").alias("Surgery___Respiratory_SystemRunning3"), 
        col("Sum_IP_Visits___Neonatal_Visits").alias("IP_Visits___Neonatal_VisitsRunning3"), 
        col("Sum_Surgery___Digestive_System").alias("Surgery___Digestive_SystemRunning3"), 
        col("Sum_Non_Standard_Benefit").alias("Non_Standard_BenefitRunning3"), 
        col("Sum_Radiology___Diagnostic___Diagnostic_Imaging")\
          .alias("Radiology___Diagnostic___Diagnostic_ImagingRunning3"), 
        col("Sum_Medical___Diagnostic___Biofeedback").alias("Medical___Diagnostic___BiofeedbackRunning3"), 
        col("Sum_Urgent_Care_Visits").alias("Urgent_Care_VisitsRunning3"), 
        col("Sum_Office_Home_Visits___Telephonic_and_Email").alias("Office_Home_Visits___Telephonic_and_EmailRunning3"), 
        col("Sum_Well_Baby_Exams").alias("Well_Baby_ExamsRunning3"), 
        col("Sum_Allergy_Testing").alias("Allergy_TestingRunning3"), 
        col("Sum_Medical___Diagnostic___Central_Nervous_System")\
          .alias("Medical___Diagnostic___Central_Nervous_SystemRunning3"), 
        col("Sum_Surgery___Musculoskeletal").alias("Surgery___MusculoskeletalRunning3"), 
        col("Sum_Immunizations").alias("ImmunizationsRunning3"), 
        col("Sum_Surgery___Auditory_System").alias("Surgery___Auditory_SystemRunning3"), 
        col("Sum_Allergy_Immunotherapy").alias("Allergy_ImmunotherapyRunning3"), 
        col("Sum_Consults").alias("ConsultsRunning3"), 
        col("Sum_PDN_HH").alias("PDN_HHRunning3"), 
        col("Sum_Maternity_Other").alias("Maternity_OtherRunning3"), 
        col("Sum_Chiropractor___Chiropractic_Manipulative_Treatment")\
          .alias("Chiropractor___Chiropractic_Manipulative_TreatmentRunning3"), 
        col("Sum_Office_Home_Visits___Care_Plan_Oversight_Services")\
          .alias("Office_Home_Visits___Care_Plan_Oversight_ServicesRunning3"), 
        col("Sum_Medical___Diagnostic___Neurology_and_Neuromuscular_Procedures")\
          .alias("Medical___Diagnostic___Neurology_and_Neuromuscular_ProceduresRunning3"), 
        col("Sum_Physical_Exams").alias("Physical_ExamsRunning3"), 
        col("Sum_DME").alias("DMERunning3"), 
        col("Sum_Surgery___Eye_and_Ocular_Adnexa").alias("Surgery___Eye_and_Ocular_AdnexaRunning3"), 
        col("Sum_Medical___Diagnostic___ENT").alias("Medical___Diagnostic___ENTRunning3"), 
        col("ER_Visits_and_Observation_Care").alias("ER_Visits_and_Observation_CareRunning3"), 
        col("Sum_Glasses_Contacts").alias("Glasses_ContactsRunning3"), 
        col("Sum_Surgery___Endocrine_System").alias("Surgery___Endocrine_SystemRunning3"), 
        col("Sum_IP_Visits___Hospital_Visits").alias("IP_Visits___Hospital_VisitsRunning3"), 
        col("Sum_Medical___Diagnostic___Vascular").alias("Medical___Diagnostic___VascularRunning3"), 
        col("Sum_IP_Visits___Newborn_Care").alias("IP_Visits___Newborn_CareRunning3"), 
        col("Sum_Miscellaneous_Medical").alias("Miscellaneous_MedicalRunning3"), 
        col("Sum_Office_Home_Visits___Rest_Home").alias("Office_Home_Visits___Rest_HomeRunning3"), 
        col("Sum_Cardiovascular___Diagnostic").alias("Cardiovascular___DiagnosticRunning3"), 
        col("Sum_Surgery___Integumentary_System").alias("Surgery___Integumentary_SystemRunning3"), 
        col("Sum_Surgery___Hemic_and_Lymphatic").alias("Surgery___Hemic_and_LymphaticRunning3"), 
        col("Sum_Hearing_Speech_Exams").alias("Hearing_Speech_ExamsRunning3"), 
        col("Sum_Non_Prescription_Drugs").alias("Non_Prescription_DrugsRunning3"), 
        col("Sum_Maternity_Deliveries").alias("Maternity_DeliveriesRunning3"), 
        col("Sum_Surgery___Male_Genital_System").alias("Surgery___Male_Genital_SystemRunning3"), 
        col("Sum_Surgery___Female_Genital_System").alias("Surgery___Female_Genital_SystemRunning3"), 
        col("Sum_Surgery___Nervous_System").alias("Surgery___Nervous_SystemRunning3"), 
        col("Sum_Cardiovascular___Noninvasive_Physiologic_Studies_and_Procedures")\
          .alias("Cardiovascular___Noninvasive_Physiologic_Studies_and_ProceduresRunning3"), 
        col("Sum_Surgery___General___Needle_Aspiration").alias("Surgery___General___Needle_AspirationRunning3"), 
        col("Sum_IP_Visits___Nursing_Facility_Services").alias("IP_Visits___Nursing_Facility_ServicesRunning3"), 
        col("Sum_Radiology___Diagnostic___Bone_Joint_Studies")\
          .alias("Radiology___Diagnostic___Bone_Joint_StudiesRunning3"), 
        col("Sum_IP_Visits___Critical_Care_Services").alias("IP_Visits___Critical_Care_ServicesRunning3"), 
        col("Sum_Surgery___Urinary_System").alias("Surgery___Urinary_SystemRunning3"), 
        col("Sum_Psychiatric_Alcohol_Drug_Abuse").alias("Psychiatric_Alcohol_Drug_AbuseRunning3"), 
        col("Sum_Cardiovascular___Cardiac_Catheterization___Injection_Procedures")\
          .alias("Cardiovascular___Cardiac_Catheterization___Injection_ProceduresRunning3"), 
        col("Sum_Radiology___Diagnostic___Other_Procedures").alias("Radiology___Diagnostic___Other_ProceduresRunning3"), 
        col("Sum_Office_Home_Visits___Office_Visits").alias("Office_Home_Visits___Office_VisitsRunning3"), 
        col("Sum_Cardiovascular___Other_Procedures").alias("Cardiovascular___Other_ProceduresRunning3"), 
        col("Sum_Surgery___Cardiovascular_System").alias("Surgery___Cardiovascular_SystemRunning3"), 
        col("Sum_Anesthesia").alias("AnesthesiaRunning3"), 
        col("Sum_Pathology_and_Lab").alias("Pathology_and_LabRunning3"), 
        col("Sum_Radiology___Therapeutic").alias("Radiology___TherapeuticRunning3"), 
        col("Sum_Medical___Diagnostic___Gastroenterology").alias("Medical___Diagnostic___GastroenterologyRunning3"), 
        col("Sum_Radiology___CT_MRI_PET").alias("Radiology___CT_MRI_PETRunning3"), 
        col("Sum_Office_Home_Visits___Prolonged_Services").alias("Office_Home_Visits___Prolonged_ServicesRunning3"), 
        col("Sum_Vision_Exams").alias("Vision_ExamsRunning3"), 
        col("Sum_Surgery___Integumentary_System___Destruction__Malignant_Lesions")\
          .alias("Surgery___Integumentary_System___Destruction__Malignant_LesionsRunning3")
    )
