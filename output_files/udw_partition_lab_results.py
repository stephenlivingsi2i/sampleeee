import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_PARTITION_LAB_RESULTS_PATH'] = 's3a://delta-lake-tables/udw/partition_lab_results.csv'
os.environ['PARTITION_LAB_RESULTS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/partition_lab_results'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_partition_lab_results_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        partition_lab_results_src_file_path = os.environ.get("UDW_PARTITION_LAB_RESULTS_PATH", "")
        if not partition_lab_results_src_file_path:
            exit_with_error(logger, f"partition_lab_results source file path should be provided!")

        partition_lab_results_delta_table_path = os.environ.get("PARTITION_LAB_RESULTS_DELTA_TABLE_PATH", "")
        if not partition_lab_results_delta_table_path:
            exit_with_error(logger, f"partition_lab_results delta table path should be provided!")

        # read partition_lab_results source csv file from s3 bucket
        logger.warn(f"read partition_lab_results source csv file from s3")
        src_df = spark.read.csv(partition_lab_results_src_file_path, header=True)
        partition_lab_results_df = src_df \
            .withColumn("clinical_result_type", get_column(src_df, "ClinicalResultType", StringType())) \
            .withColumn("order_id", get_column(src_df, "ORDER_ID", IntegerType())) \
            .withColumn("placer_entity_identifier", get_column(src_df, "PLACER_ENTITY_IDENTIFIER", StringType())) \
            .withColumn("placer_namespace_id", get_column(src_df, "PLACER_NAMESPACE_ID", StringType())) \
            .withColumn("transaction_date", get_column(src_df, "TRANSACTION_DATE", DateType())) \
            .withColumn("order_status", get_column(src_df, "ORDER_STATUS", StringType())) \
            .withColumn("order_control", get_column(src_df, "ORDER_CONTROL", StringType())) \
            .withColumn("order_effective_date", get_column(src_df, "ORDER_EFFECTIVE_DATE", DateType())) \
            .withColumn("hospitalization_id", get_column(src_df, "HOSPITALIZATION_ID", IntegerType())) \
            .withColumn("filler_entity_identifier", get_column(src_df, "FILLER_ENTITY_IDENTIFIER", StringType())) \
            .withColumn("filler_namespace_id", get_column(src_df, "FILLER_NAMESPACE_ID", StringType())) \
            .withColumn("accession_number", get_column(src_df, "AccessionNumber", StringType())) \
            .withColumn("physician_id", get_column(src_df, "PHYSICIAN_ID", IntegerType())) \
            .withColumn("lab_test_id", get_column(src_df, "LABTEST_ID", IntegerType())) \
            .withColumn("lab_test_type_code", get_column(src_df, "LABTEST_TYPE_CODE", StringType())) \
            .withColumn("lab_test_type_other", get_column(src_df, "LABTEST_TYPE_OTHER", StringType())) \
            .withColumn("lab_test_date", get_column(src_df, "LABTEST_DATE", DateType())) \
            .withColumn("source_entry", get_column(src_df, "SOURCE_ENTRY", StringType())) \
            .withColumn("specimen_received_date", get_column(src_df, "SPECIMEN_RECEIVED_DATE", DateType())) \
            .withColumn("specimen_source_name_or_code", get_column(src_df, "SPECIMEN_SOURCE_NAME_OR_CODE", StringType())) \
            .withColumn("observation_schedule_date", get_column(src_df, "OBSERVATION_SCHEDULE_DATE", DateType())) \
            .withColumn("observation_date", get_column(src_df, "OBSERVATION_DATE", DateType())) \
            .withColumn("observation_comment_source", get_column(src_df, "OBSERVATION_COMMENT_SOURCE", StringType())) \
            .withColumn("observation_comment", get_column(src_df, "OBSERVATION_COMMENT", StringType())) \
            .withColumn("lab_test_type_desc", get_column(src_df, "LABTEST_TYPE_DESC", StringType())) \
            .withColumn("lab_test_coding_system_name", get_column(src_df, "LABTEST_CODING_SYSTEM_NAME", StringType())) \
            .withColumn("specimen_action_code", get_column(src_df, "SPECIMEN_ACTION_CODE", StringType())) \
            .withColumn("collected_by", get_column(src_df, "COLLECTED_BY", StringType())) \
            .withColumn("test_status", get_column(src_df, "TEST_STATUS", StringType())) \
            .withColumn("specimen_source_text", get_column(src_df, "SPECIMEN_SOURCE_TEXT", StringType())) \
            .withColumn("attending_physician_id", get_column(src_df, "ATTENDING_PHYSICIAN_ID", IntegerType())) \
            .withColumn("ordering_physician_id", get_column(src_df, "ORDERING_PHYSICIAN_ID", IntegerType())) \
            .withColumn("radiologist_id", get_column(src_df, "RADIOLOGIST_ID", IntegerType())) \
            .withColumn("lab_test_physician_id", get_column(src_df, "LABTEST_PHYSICIAN_ID", IntegerType())) \
            .withColumn("obr_notes", get_column(src_df, "OBR_NOTES", StringType())) \
            .withColumn("cpt_code", get_column(src_df, "CPT_CODE", StringType())) \
            .withColumn("loinc_code", get_column(src_df, "LOINC_CODE", StringType())) \
            .withColumn("lab_test_result_id", get_column(src_df, "LABTEST_RESULT_ID", IntegerType())) \
            .withColumn("lab_test_code", get_column(src_df, "LABTEST_CODE", StringType())) \
            .withColumn("lab_test_code_other", get_column(src_df, "LABTEST_CODE_OTHER", StringType())) \
            .withColumn("lab_test_reason", get_column(src_df, "LABTEST_REASON", StringType())) \
            .withColumn("lab_test_result_code", get_column(src_df, "LABTEST_RESULT_CODE", StringType())) \
            .withColumn("units", get_column(src_df, "UNITS", StringType())) \
            .withColumn("lab_test_result_date", get_column(src_df, "LABTEST_RESULT_DATE", DateType())) \
            .withColumn("lab_test_reference_range", get_column(src_df, "LABTEST_REFERENCE_RANGE", StringType())) \
            .withColumn("result_status", get_column(src_df, "RESULT_STATUS", StringType())) \
            .withColumn("lab_test_result_desc", get_column(src_df, "LABTEST_RESULT_DESC", StringType())) \
            .withColumn("abnormal_test_type", get_column(src_df, "ABNORMAL_TEST_TYPE", StringType())) \
            .withColumn("lab_test_result_value", get_column(src_df, "LABTEST_RESULT_VALUE", StringType())) \
            .withColumn("lab_test_notes", get_column(src_df, "LABTEST_NOTES", StringType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", StringType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .withColumn("lab_test_alternate_code", get_column(src_df, "LABTEST_ALTERNATE_CODE", StringType())) \
            .withColumn("lab_test_alternate_coding_system_name", get_column(src_df, "LABTEST_ALTERNATE_CODING_SYSTEM_NAME", StringType())) \
            .withColumn("lab_test_alternate_result_desc", get_column(src_df, "LABTEST_ALTERNATE_RESULT_DESC", StringType())) \
            .withColumn("lab_company_id", get_column(src_df, "LABCOMPANY_ID", IntegerType())) \
            .withColumn("lab_company_name", get_column(src_df, "LAB_COMPANY_NAME", StringType())) \
            .withColumn("lab_test_result_coding_system_name", get_column(src_df, "LABTEST_RESULT_CODING_SYSTEM_NAME", StringType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("admission_date", get_column(src_df, "ADMISSION_DATE", DateType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "clinical_result_type",
                "order_id",
                "placer_entity_identifier",
                "placer_namespace_id",
                "transaction_date",
                "order_status",
                "order_control",
                "order_effective_date",
                "hospitalization_id",
                "filler_entity_identifier",
                "filler_namespace_id",
                "accession_number",
                "physician_id",
                "lab_test_id",
                "lab_test_type_code",
                "lab_test_type_other",
                "lab_test_date",
                "source_entry",
                "specimen_received_date",
                "specimen_source_name_or_code",
                "observation_schedule_date",
                "observation_date",
                "observation_comment_source",
                "observation_comment",
                "lab_test_type_desc",
                "lab_test_coding_system_name",
                "specimen_action_code",
                "collected_by",
                "test_status",
                "specimen_source_text",
                "attending_physician_id",
                "ordering_physician_id",
                "radiologist_id",
                "lab_test_physician_id",
                "obr_notes",
                "cpt_code",
                "loinc_code",
                "lab_test_result_id",
                "lab_test_code",
                "lab_test_code_other",
                "lab_test_reason",
                "lab_test_result_code",
                "units",
                "lab_test_result_date",
                "lab_test_reference_range",
                "result_status",
                "lab_test_result_desc",
                "abnormal_test_type",
                "lab_test_result_value",
                "lab_test_notes",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date",
                "lab_test_alternate_code",
                "lab_test_alternate_coding_system_name",
                "lab_test_alternate_result_desc",
                "lab_company_id",
                "lab_company_name",
                "lab_test_result_coding_system_name",
                "health_record_key",
                "health_token_id",
                "admission_date",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.partition_lab_results (
                clinical_result_type STRING,
                order_id INTEGER,
                placer_entity_identifier STRING,
                placer_namespace_id STRING,
                transaction_date DATE,
                order_status STRING,
                order_control STRING,
                order_effective_date DATE,
                hospitalization_id INTEGER,
                filler_entity_identifier STRING,
                filler_namespace_id STRING,
                accession_number STRING,
                physician_id INTEGER,
                lab_test_id INTEGER,
                lab_test_type_code STRING,
                lab_test_type_other STRING,
                lab_test_date DATE,
                source_entry STRING,
                specimen_received_date DATE,
                specimen_source_name_or_code STRING,
                observation_schedule_date DATE,
                observation_date DATE,
                observation_comment_source STRING,
                observation_comment STRING,
                lab_test_type_desc STRING,
                lab_test_coding_system_name STRING,
                specimen_action_code STRING,
                collected_by STRING,
                test_status STRING,
                specimen_source_text STRING,
                attending_physician_id INTEGER,
                ordering_physician_id INTEGER,
                radiologist_id INTEGER,
                lab_test_physician_id INTEGER,
                obr_notes STRING,
                cpt_code STRING,
                loinc_code STRING,
                lab_test_result_id INTEGER,
                lab_test_code STRING,
                lab_test_code_other STRING,
                lab_test_reason STRING,
                lab_test_result_code STRING,
                units STRING,
                lab_test_result_date DATE,
                lab_test_reference_range STRING,
                result_status STRING,
                lab_test_result_desc STRING,
                abnormal_test_type STRING,
                lab_test_result_value STRING,
                lab_test_notes STRING,
                created_by STRING,
                created_date TIMESTAMP,
                updated_by STRING,
                updated_date TIMESTAMP,
                lab_test_alternate_code STRING,
                lab_test_alternate_coding_system_name STRING,
                lab_test_alternate_result_desc STRING,
                lab_company_id INTEGER,
                lab_company_name STRING,
                lab_test_result_coding_system_name STRING,
                health_record_key INTEGER,
                health_token_id BINARY,
                admission_date DATE,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/partition_lab_results'
        """)

        # write data into delta lake partition_lab_results table
        partition_lab_results_df.write.format("delta").mode("append").save(partition_lab_results_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
