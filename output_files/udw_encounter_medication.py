import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_ENCOUNTER_MEDICATION_PATH'] = 's3a://delta-lake-tables/udw/encounter_medication.csv'
os.environ['ENCOUNTER_MEDICATION_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/encounter_medication'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_encounter_medication_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        encounter_medication_src_file_path = os.environ.get("UDW_ENCOUNTER_MEDICATION_PATH", "")
        if not encounter_medication_src_file_path:
            exit_with_error(logger, f"encounter_medication source file path should be provided!")

        encounter_medication_delta_table_path = os.environ.get("ENCOUNTER_MEDICATION_DELTA_TABLE_PATH", "")
        if not encounter_medication_delta_table_path:
            exit_with_error(logger, f"encounter_medication delta table path should be provided!")

        # read encounter_medication source csv file from s3 bucket
        logger.warn(f"read encounter_medication source csv file from s3")
        src_df = spark.read.csv(encounter_medication_src_file_path, header=True)
        encounter_medication_df = src_df \
            .withColumn("transaction_id", get_column(src_df, "TRANSACTION_ID", LongType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", IntegerType())) \
            .withColumn("prescriber_name", get_column(src_df, "PRESCRIBER_NAME", StringType())) \
            .withColumn("rx_status_code", get_column(src_df, "RX_STATUS_CODE", StringType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("drug_description", get_column(src_df, "DRUGDESCRIPTION", StringType())) \
            .withColumn("product_code", get_column(src_df, "PRODUCTCODE", StringType())) \
            .withColumn("product_code_qualifier", get_column(src_df, "PRODUCTCODEQUALIFIER", StringType())) \
            .withColumn("quantity", get_column(src_df, "QUANTITY", StringType())) \
            .withColumn("quantity_qualifier", get_column(src_df, "QUANTITYQUALIFIER", StringType())) \
            .withColumn("dosage_form", get_column(src_df, "DOSAGEFORM", StringType())) \
            .withColumn("strength", get_column(src_df, "STRENGTH", StringType())) \
            .withColumn("strength_units", get_column(src_df, "STRENGTHUNITS", StringType())) \
            .withColumn("status", get_column(src_df, "STATUS", StringType())) \
            .withColumn("days_supply", get_column(src_df, "DAYSSUPPLY", StringType())) \
            .withColumn("substitutions", get_column(src_df, "SUBSTITUTIONS", StringType())) \
            .withColumn("refills", get_column(src_df, "REFILLS", StringType())) \
            .withColumn("directions", get_column(src_df, "DIRECTIONS", StringType())) \
            .withColumn("diagnosis_primary", get_column(src_df, "DIAGNOSISPRIMARY", StringType())) \
            .withColumn("diagnosis_p_qualifier", get_column(src_df, "DIAGNOSISPQUALIFIER", StringType())) \
            .withColumn("diagnosis_secondary", get_column(src_df, "DIAGNOSISSECONDARY", StringType())) \
            .withColumn("diagnosis_s_qualifier", get_column(src_df, "DIAGNOSISSQUALIFIER", StringType())) \
            .withColumn("last_fill_date", get_column(src_df, "LASTFILLDATE", DateType())) \
            .withColumn("written_date", get_column(src_df, "WRITTENDATE", DateType())) \
            .withColumn("scheduled", get_column(src_df, "SCHEDULED", StringType())) \
            .withColumn("dose", get_column(src_df, "DOSE", StringType())) \
            .withColumn("frequency", get_column(src_df, "FREQUENCY", StringType())) \
            .withColumn("dosage_form_name", get_column(src_df, "DOSAGEFORMNAME", StringType())) \
            .withColumn("route", get_column(src_df, "ROUTE", StringType())) \
            .withColumn("formulary_status", get_column(src_df, "FORMULARY_STATUS", StringType())) \
            .withColumn("source_code_id", get_column(src_df, "SOURCE_CODE_ID", IntegerType())) \
            .withColumn("medication_type_code", get_column(src_df, "MEDICATION_TYPE_CODE", IntegerType())) \
            .withColumn("medi_type_oth_desc", get_column(src_df, "MEDI_TYPE_OTH_DESC", StringType())) \
            .withColumn("reason_for_medication", get_column(src_df, "REASON_FOR_MEDICATION", StringType())) \
            .withColumn("medi_start_date", get_column(src_df, "MEDI_START_DATE", DateType())) \
            .withColumn("medi_end_date", get_column(src_df, "MEDI_END_DATE", DateType())) \
            .withColumn("refill_date", get_column(src_df, "REFILL_DATE", DateType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .withColumn("route_code", get_column(src_df, "ROUTE_CODE", StringType())) \
            .withColumn("route_code_qualifer", get_column(src_df, "ROUTE_CODE_QUALIFER", StringType())) \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("hospitalization_id", get_column(src_df, "HOSPITALIZATION_ID", IntegerType())) \
            .withColumn("ndc_code", get_column(src_df, "NDC_CODE", StringType())) \
            .withColumn("generic_name", get_column(src_df, "GenericName", StringType())) \
            .withColumn("units", get_column(src_df, "UNITS", StringType())) \
            .withColumn("shape", get_column(src_df, "SHAPE", StringType())) \
            .withColumn("color", get_column(src_df, "COLOR", StringType())) \
            .withColumn("duration", get_column(src_df, "DURATION", IntegerType())) \
            .withColumn("physician_name", get_column(src_df, "PHYSICIAN_NAME", StringType())) \
            .withColumn("hospital_name", get_column(src_df, "HOSPITAL_NAME", StringType())) \
            .withColumn("instruction", get_column(src_df, "INSTRUCTION", StringType())) \
            .withColumn("type", get_column(src_df, "TYPE", StringType())) \
            .withColumn("days", get_column(src_df, "DAYS", StringType())) \
            .withColumn("med_count", get_column(src_df, "MED_COUNT", IntegerType())) \
            .withColumn("earliest_date", get_column(src_df, "EARLIEST_DATE", DateType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .select([
                "transaction_id",
                "hie_source_code",
                "prescriber_name",
                "rx_status_code",
                "health_record_key",
                "health_token_id",
                "drug_description",
                "product_code",
                "product_code_qualifier",
                "quantity",
                "quantity_qualifier",
                "dosage_form",
                "strength",
                "strength_units",
                "status",
                "days_supply",
                "substitutions",
                "refills",
                "directions",
                "diagnosis_primary",
                "diagnosis_p_qualifier",
                "diagnosis_secondary",
                "diagnosis_s_qualifier",
                "last_fill_date",
                "written_date",
                "scheduled",
                "dose",
                "frequency",
                "dosage_form_name",
                "route",
                "formulary_status",
                "source_code_id",
                "medication_type_code",
                "medi_type_oth_desc",
                "reason_for_medication",
                "medi_start_date",
                "medi_end_date",
                "refill_date",
                "created_date",
                "updated_date",
                "route_code",
                "route_code_qualifer",
                "id",
                "hospitalization_id",
                "ndc_code",
                "generic_name",
                "units",
                "shape",
                "color",
                "duration",
                "physician_name",
                "hospital_name",
                "instruction",
                "type",
                "days",
                "med_count",
                "earliest_date",
                "client_id",
                "client_name",
                "hash_bytes_sha",
                "target_created_date",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.encounter_medication (
                transaction_id LONG,
                hie_source_code INTEGER,
                prescriber_name STRING,
                rx_status_code STRING,
                health_record_key INTEGER,
                health_token_id BINARY,
                drug_description STRING,
                product_code STRING,
                product_code_qualifier STRING,
                quantity STRING,
                quantity_qualifier STRING,
                dosage_form STRING,
                strength STRING,
                strength_units STRING,
                status STRING,
                days_supply STRING,
                substitutions STRING,
                refills STRING,
                directions STRING,
                diagnosis_primary STRING,
                diagnosis_p_qualifier STRING,
                diagnosis_secondary STRING,
                diagnosis_s_qualifier STRING,
                last_fill_date DATE,
                written_date DATE,
                scheduled STRING,
                dose STRING,
                frequency STRING,
                dosage_form_name STRING,
                route STRING,
                formulary_status STRING,
                source_code_id INTEGER,
                medication_type_code INTEGER,
                medi_type_oth_desc STRING,
                reason_for_medication STRING,
                medi_start_date DATE,
                medi_end_date DATE,
                refill_date DATE,
                created_date TIMESTAMP,
                updated_date TIMESTAMP,
                route_code STRING,
                route_code_qualifer STRING,
                id INTEGER,
                hospitalization_id INTEGER,
                ndc_code STRING,
                generic_name STRING,
                units STRING,
                shape STRING,
                color STRING,
                duration INTEGER,
                physician_name STRING,
                hospital_name STRING,
                instruction STRING,
                type STRING,
                days STRING,
                med_count INTEGER,
                earliest_date DATE,
                client_id INTEGER,
                client_name STRING,
                hash_bytes_sha BINARY,
                target_created_date TIMESTAMP
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/encounter_medication'
        """)

        # write data into delta lake encounter_medication table
        encounter_medication_df.write.format("delta").mode("append").save(encounter_medication_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
