from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_2683(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("YEARMONTH"))
    df2 = df1.pivot(
        "`Family Description`",
        ["IP_Visits___Hospital_Visits",  "DME",  "Vision_Exams",  "IP_Visits___Newborn_Care",  "Surgery___Male_Genital_System",          "Surgery___Integumentary_System___Excision_of_Malignant_Lesions",  "Consults",  "Glasses_Contacts",          "Medical___Diagnostic___Biofeedback",  "Radiology___Diagnostic___Other_Procedures",          "Medical___Diagnostic___Central_Nervous_System",  "Office_Home_Visits___Prolonged_Services",          "Surgery___Integumentary_System___Destruction__Benign_or_Premalignant_Lesions",          "Psychiatric_Alcohol_Drug_Abuse",  "Anesthesia",  "Office_Home_Visits___Telephonic_and_Email",          "Cardiovascular___Noninvasive_Physiologic_Studies_and_Procedures",  "PDN_HH",  "Radiology___Therapeutic",          "Pathology_and_Lab",  "Surgery___Urinary_System",  "Cardiovascular___Diagnostic",          "Cardiovascular___Cardiac_Catheterization___Injection_Procedures",  "ER_Visits_and_Observation_Care",          "Surgery___Endocrine_System",  "Surgery___Integumentary_System",  "Hearing_Speech_Exams",  "Maternity_Other",          "Allergy_Immunotherapy",  "Radiology___CT_MRI_PET",  "IP_Visits___Neonatal_Visits",          "Medical___Diagnostic___Vascular",  "Allergy_Testing",  "IP_Visits___Critical_Care_Services",          "Non_Standard_Benefit",  "Surgery___Musculoskeletal",  "IP_Visits___Nursing_Facility_Services",          "Surgery___General___Needle_Aspiration",  "Immunizations",  "Well_Baby_Exams",          "Cardiovascular___Other_Procedures",  "Urgent_Care_Visits",  "Surgery___Cardiovascular_System",          "Surgery___Female_Genital_System",  "Radiology___Diagnostic___Diagnostic_Imaging",          "Surgery___Integumentary_System___Destruction__Malignant_Lesions",  "Radiology___Nuclear",          "Non_Prescription_Drugs",  "Surgery___Auditory_System",  "Maternity_Deliveries",  "Physical_Exams",          "Miscellaneous_Medical",  "Surgery___Respiratory_System",  "Radiology___Diagnostic___Bone_Joint_Studies",          "Chiropractor___Chiropractic_Manipulative_Treatment",  "Medical___Diagnostic___ENT",          "Office_Home_Visits___Office_Visits",  "Medical___Diagnostic___Gastroenterology",          "Office_Home_Visits___Care_Plan_Oversight_Services",          "Medical___Diagnostic___Neurology_and_Neuromuscular_Procedures",  "Surgery___Nervous_System",          "Physical_Therapy",  "Surgery___Digestive_System",  "Office_Home_Visits___Rest_Home",          "Surgery___Eye_and_Ocular_Adnexa",  "Surgery___Hemic_and_Lymphatic"]
    )

    return df2.agg(sum(col("Sum_PROC_COUNT")).alias("Sum_PROC_COUNT"))
