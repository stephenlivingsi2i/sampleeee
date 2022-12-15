import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_ENCOUNTER_DETAILS_PATH'] = 's3a://delta-lake-tables/udw/encounter_details.csv'
os.environ['ENCOUNTER_DETAILS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/encounter_details'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_encounter_details_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        encounter_details_src_file_path = os.environ.get("UDW_ENCOUNTER_DETAILS_PATH", "")
        if not encounter_details_src_file_path:
            exit_with_error(logger, f"encounter_details source file path should be provided!")

        encounter_details_delta_table_path = os.environ.get("ENCOUNTER_DETAILS_DELTA_TABLE_PATH", "")
        if not encounter_details_delta_table_path:
            exit_with_error(logger, f"encounter_details delta table path should be provided!")

        # read encounter_details source csv file from s3 bucket
        logger.warn(f"read encounter_details source csv file from s3")
        src_df = spark.read.csv(encounter_details_src_file_path, header=True)
        encounter_details_df = src_df \
            .withColumn("hospitalization_id", get_column(src_df, "HOSPITALIZATION_ID", IntegerType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("hospitalization_type_code", get_column(src_df, "HOSPITALIZATION_TYPE_CODE", StringType())) \
            .withColumn("hospitalization_type_oth_desc", get_column(src_df, "HOSPITALIZATION_TYPE_OTH_DESC", StringType())) \
            .withColumn("hospital_name", get_column(src_df, "HOSPITALNAME", StringType())) \
            .withColumn("admission_date", get_column(src_df, "ADMISSION_DATE", DateType())) \
            .withColumn("hospital_service", get_column(src_df, "HOSPITAL_SERVICE", StringType())) \
            .withColumn("admit_source", get_column(src_df, "ADMIT_SOURCE", StringType())) \
            .withColumn("patient_type", get_column(src_df, "PATIENT_TYPE", StringType())) \
            .withColumn("discharge_disposition", get_column(src_df, "DISCHARGE_DISPOSITION", StringType())) \
            .withColumn("financial_class", get_column(src_df, "FINANCIAL_CLASS", StringType())) \
            .withColumn("admission_type", get_column(src_df, "ADMISSION_TYPE", StringType())) \
            .withColumn("assigned_patient_location", get_column(src_df, "ASSIGNED_PATIENT_LOCATION", StringType())) \
            .withColumn("preadmit_number", get_column(src_df, "PREADMIT_NUMBER", StringType())) \
            .withColumn("prior_patient_location", get_column(src_df, "PRIOR_PATIENT_LOCATION", StringType())) \
            .withColumn("temporary_location", get_column(src_df, "TEMPORARY_LOCATION", StringType())) \
            .withColumn("preadmit_test_indicator", get_column(src_df, "PREADMIT_TEST_INDICATOR", StringType())) \
            .withColumn("re_admission_indicator", get_column(src_df, "RE_ADMISSION_INDICATOR", StringType())) \
            .withColumn("ambulatory_status", get_column(src_df, "AMBULATORY_STATUS", StringType())) \
            .withColumn("vip_indicator", get_column(src_df, "VIP_INDICATOR", StringType())) \
            .withColumn("visit_number", get_column(src_df, "VISIT_NUMBER", StringType())) \
            .withColumn("charge_price_indicator", get_column(src_df, "CHARGE_PRICE_INDICATOR", StringType())) \
            .withColumn("courtesy_code", get_column(src_df, "COURTESY_CODE", StringType())) \
            .withColumn("credit_rating", get_column(src_df, "CREDIT_RATING", StringType())) \
            .withColumn("contract_code", get_column(src_df, "CONTRACT_CODE", StringType())) \
            .withColumn("discharge_date", get_column(src_df, "DISCHARGE_DATE", DateType())) \
            .withColumn("contract_effective_date", get_column(src_df, "CONTRACT_EFFECTIVE_DATE", DateType())) \
            .withColumn("contract_amount", get_column(src_df, "CONTRACT_AMOUNT", DecimalType())) \
            .withColumn("contract_period", get_column(src_df, "CONTRACT_PERIOD", StringType())) \
            .withColumn("interest_code", get_column(src_df, "INTEREST_CODE", StringType())) \
            .withColumn("transfer_to_bad_debt_code", get_column(src_df, "TRANSFER_TO_BAD_DEBT_CODE", StringType())) \
            .withColumn("transfer_to_bad_debt_date", get_column(src_df, "TRANSFER_TO_BAD_DEBT_DATE", DateType())) \
            .withColumn("bad_debt_agency_code", get_column(src_df, "BAD_DEBT_AGENCY_CODE", StringType())) \
            .withColumn("bad_debt_transfer_amount", get_column(src_df, "BAD_DEBT_TRANSFER_AMOUNT", DecimalType())) \
            .withColumn("bad_debt_recovery_amount", get_column(src_df, "BAD_DEBT_RECOVERY_AMOUNT", DecimalType())) \
            .withColumn("delete_account_indicator", get_column(src_df, "DELETE_ACCOUNT_INDICATOR", StringType())) \
            .withColumn("delete_account_date", get_column(src_df, "DELETE_ACCOUNT_DATE", DateType())) \
            .withColumn("discharged_to_location", get_column(src_df, "DISCHARGED_TO_LOCATION", StringType())) \
            .withColumn("diet_type", get_column(src_df, "DIET_TYPE", StringType())) \
            .withColumn("bed_status", get_column(src_df, "BED_STATUS", StringType())) \
            .withColumn("account_status", get_column(src_df, "ACCOUNT_STATUS", StringType())) \
            .withColumn("pending_location", get_column(src_df, "PENDING_LOCATION", StringType())) \
            .withColumn("prior_temporary_location", get_column(src_df, "PRIOR_TEMPORARY_LOCATION", StringType())) \
            .withColumn("current_patient_balance", get_column(src_df, "CURRENT_PATIENT_BALANCE", DecimalType())) \
            .withColumn("total_charges", get_column(src_df, "TOTAL_CHARGES", DecimalType())) \
            .withColumn("total_adjustments", get_column(src_df, "TOTAL_ADJUSTMENTS", DecimalType())) \
            .withColumn("total_payments", get_column(src_df, "TOTAL_PAYMENTS", DecimalType())) \
            .withColumn("alternate_visit_id", get_column(src_df, "ALTERNATE_VISIT_ID", StringType())) \
            .withColumn("visit_indicator", get_column(src_df, "VISIT_INDICATOR", StringType())) \
            .withColumn("other_healthcare_provider", get_column(src_df, "OTHER_HEALTHCARE_PROVIDER", StringType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", StringType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .withColumn("source_entry", get_column(src_df, "source_entry", StringType())) \
            .withColumn("insurance_id", get_column(src_df, "INSURANCE_ID", IntegerType())) \
            .withColumn("servicing_facility", get_column(src_df, "SERVICING_FACILITY", StringType())) \
            .withColumn("episode_id", get_column(src_df, "EPISODE_ID", StringType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("physician_id", get_column(src_df, "PHYSICIAN_ID", StringType())) \
            .withColumn("diagnosis_code", get_column(src_df, "DIAGNOSIS_CODE", StringType())) \
            .withColumn("diagnosis_oth_desc", get_column(src_df, "DIAGNOSIS_OTH_DESC", StringType())) \
            .withColumn("notes_type", get_column(src_df, "NOTESTYPE", StringType())) \
            .withColumn("notes", get_column(src_df, "NOTES", StringType())) \
            .withColumn("procedure_code", get_column(src_df, "PROCEDURE_CODE", StringType())) \
            .withColumn("procedure_oth_desc", get_column(src_df, "PROCEDURE_OTH_DESC", StringType())) \
            .withColumn("product", get_column(src_df, "PRODUCT", StringType())) \
            .withColumn("auth_type", get_column(src_df, "AUTH_TYPE", StringType())) \
            .withColumn("acute", get_column(src_df, "ACUTE", StringType())) \
            .withColumn("sub", get_column(src_df, "SUB", StringType())) \
            .withColumn("snf", get_column(src_df, "SNF", StringType())) \
            .withColumn("denied", get_column(src_df, "DENIED", StringType())) \
            .withColumn("encounter_reason", get_column(src_df, "Encounter_reason", StringType())) \
            .withColumn("visit_status", get_column(src_df, "VISIT_STATUS", StringType())) \
            .withColumn("visit_type_ecw", get_column(src_df, "VISIT_TYPE_ECW", StringType())) \
            .withColumn("medication_reconcilation_flag", get_column(src_df, "MEDICATION_RECONCILATION_FLAG", StringType())) \
            .withColumn("poscode_description", get_column(src_df, "POSCODE_DESCRIPTION", StringType())) \
            .withColumn("place_of_service", get_column(src_df, "PlaceOfService", StringType())) \
            .withColumn("rehab", get_column(src_df, "Rehab", StringType())) \
            .withColumn("nursing_home", get_column(src_df, "Nursing_Home", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("practice_name", get_column(src_df, "PracticeName", StringType())) \
            .withColumn("source_id", get_column(src_df, "SOURCEID", IntegerType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "hospitalization_id",
                "health_record_key",
                "health_token_id",
                "hospitalization_type_code",
                "hospitalization_type_oth_desc",
                "hospital_name",
                "admission_date",
                "hospital_service",
                "admit_source",
                "patient_type",
                "discharge_disposition",
                "financial_class",
                "admission_type",
                "assigned_patient_location",
                "preadmit_number",
                "prior_patient_location",
                "temporary_location",
                "preadmit_test_indicator",
                "re_admission_indicator",
                "ambulatory_status",
                "vip_indicator",
                "visit_number",
                "charge_price_indicator",
                "courtesy_code",
                "credit_rating",
                "contract_code",
                "discharge_date",
                "contract_effective_date",
                "contract_amount",
                "contract_period",
                "interest_code",
                "transfer_to_bad_debt_code",
                "transfer_to_bad_debt_date",
                "bad_debt_agency_code",
                "bad_debt_transfer_amount",
                "bad_debt_recovery_amount",
                "delete_account_indicator",
                "delete_account_date",
                "discharged_to_location",
                "diet_type",
                "bed_status",
                "account_status",
                "pending_location",
                "prior_temporary_location",
                "current_patient_balance",
                "total_charges",
                "total_adjustments",
                "total_payments",
                "alternate_visit_id",
                "visit_indicator",
                "other_healthcare_provider",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date",
                "source_entry",
                "insurance_id",
                "servicing_facility",
                "episode_id",
                "hie_source_code",
                "physician_id",
                "diagnosis_code",
                "diagnosis_oth_desc",
                "notes_type",
                "notes",
                "procedure_code",
                "procedure_oth_desc",
                "product",
                "auth_type",
                "acute",
                "sub",
                "snf",
                "denied",
                "encounter_reason",
                "visit_status",
                "visit_type_ecw",
                "medication_reconcilation_flag",
                "poscode_description",
                "place_of_service",
                "rehab",
                "nursing_home",
                "client_id",
                "client_name",
                "practice_name",
                "source_id",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.encounter_details (
                hospitalization_id INTEGER,
                health_record_key INTEGER,
                health_token_id BINARY,
                hospitalization_type_code STRING,
                hospitalization_type_oth_desc STRING,
                hospital_name STRING,
                admission_date DATE,
                hospital_service STRING,
                admit_source STRING,
                patient_type STRING,
                discharge_disposition STRING,
                financial_class STRING,
                admission_type STRING,
                assigned_patient_location STRING,
                preadmit_number STRING,
                prior_patient_location STRING,
                temporary_location STRING,
                preadmit_test_indicator STRING,
                re_admission_indicator STRING,
                ambulatory_status STRING,
                vip_indicator STRING,
                visit_number STRING,
                charge_price_indicator STRING,
                courtesy_code STRING,
                credit_rating STRING,
                contract_code STRING,
                discharge_date DATE,
                contract_effective_date DATE,
                contract_amount DECIMAL,
                contract_period STRING,
                interest_code STRING,
                transfer_to_bad_debt_code STRING,
                transfer_to_bad_debt_date DATE,
                bad_debt_agency_code STRING,
                bad_debt_transfer_amount DECIMAL,
                bad_debt_recovery_amount DECIMAL,
                delete_account_indicator STRING,
                delete_account_date DATE,
                discharged_to_location STRING,
                diet_type STRING,
                bed_status STRING,
                account_status STRING,
                pending_location STRING,
                prior_temporary_location STRING,
                current_patient_balance DECIMAL,
                total_charges DECIMAL,
                total_adjustments DECIMAL,
                total_payments DECIMAL,
                alternate_visit_id STRING,
                visit_indicator STRING,
                other_healthcare_provider STRING,
                created_by STRING,
                created_date TIMESTAMP,
                updated_by STRING,
                updated_date TIMESTAMP,
                source_entry STRING,
                insurance_id INTEGER,
                servicing_facility STRING,
                episode_id STRING,
                hie_source_code STRING,
                physician_id STRING,
                diagnosis_code STRING,
                diagnosis_oth_desc STRING,
                notes_type STRING,
                notes STRING,
                procedure_code STRING,
                procedure_oth_desc STRING,
                product STRING,
                auth_type STRING,
                acute STRING,
                sub STRING,
                snf STRING,
                denied STRING,
                encounter_reason STRING,
                visit_status STRING,
                visit_type_ecw STRING,
                medication_reconcilation_flag STRING,
                poscode_description STRING,
                place_of_service STRING,
                rehab STRING,
                nursing_home STRING,
                client_id INTEGER,
                client_name STRING,
                practice_name STRING,
                source_id INTEGER,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY   
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/encounter_details'
        """)

        # write data into delta lake encounter_details table
        encounter_details_df.write.format("delta").mode("append").save(encounter_details_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
