import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_BENEFICIARY_DETAILS_PATH'] = 's3a://delta-lake-tables/udw/beneficiary_details.csv'
os.environ['BENEFICIARY_DETAILS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/beneficiary-details'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_beneficiary_details_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        beneficiary_details_src_file_path = os.environ.get("UDW_BENEFICIARY_DETAILS_PATH", "")
        if not beneficiary_details_src_file_path:
            exit_with_error(logger, f"beneficiary_details source file path should be provided!")

        beneficiary_details_delta_table_path = os.environ.get("BENEFICIARY_DETAILS_DELTA_TABLE_PATH", "")
        if not beneficiary_details_delta_table_path:
            exit_with_error(logger, f"beneficiary_details delta table path should be provided!")

        # read practice source csv file from s3 bucket
        logger.warn(f"read beneficiary_details source csv file from s3")
        src_df = spark.read.csv(beneficiary_details_src_file_path, header=True)
        beneficiary_details = src_df \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("health_record_key", get_column(src_df, "healthrecordkey", IntegerType())) \
            .withColumn("medicare", get_column(src_df, "MEDICARE", StringType())) \
            .withColumn("dob", get_column(src_df, "dob", DateType())) \
            .withColumn("ssn", get_column(src_df, "ssn", StringType())) \
            .withColumn("mothers_maiden_name", get_column(src_df, "mothers_maiden_name", StringType())) \
            .withColumn("street_address1", get_column(src_df, "streetaddress1", StringType())) \
            .withColumn("street_address2", get_column(src_df, "streetaddress2", StringType())) \
            .withColumn("city", get_column(src_df, "city", StringType())) \
            .withColumn("state", get_column(src_df, "STATE", StringType())) \
            .withColumn("zip", get_column(src_df, "zip", StringType())) \
            .withColumn("phone_home", get_column(src_df, "phone_home", StringType())) \
            .withColumn("phone_work", get_column(src_df, "phone_work", StringType())) \
            .withColumn("phone_mobile", get_column(src_df, "PHONE_MOBILE", StringType())) \
            .withColumn("email", get_column(src_df, "email", StringType())) \
            .withColumn("fax", get_column(src_df, "FAX", StringType())) \
            .withColumn("last_name", get_column(src_df, "lastname", StringType())) \
            .withColumn("first_name", get_column(src_df, "firstname", StringType())) \
            .withColumn("middle_initials", get_column(src_df, "middleinitials", StringType())) \
            .withColumn("sex", get_column(src_df, "sex", StringType())) \
            .withColumn("marital_status", get_column(src_df, "marital_status", StringType())) \
            .withColumn("nationality", get_column(src_df, "nationality", StringType())) \
            .withColumn("title", get_column(src_df, "title", StringType())) \
            .withColumn("birth_place", get_column(src_df, "BIRTH_PLACE", StringType())) \
            .withColumn("primary_language", get_column(src_df, "PRIMARY_LANGUAGE", StringType())) \
            .withColumn("secondary_language", get_column(src_df, "SECONDARY_LANGUAGE", StringType())) \
            .withColumn("alias", get_column(src_df, "ALIAS", StringType())) \
            .withColumn("alert_desc", get_column(src_df, "ALERT_DESC", StringType())) \
            .withColumn("race_description", get_column(src_df, "RACE_DESCRIPTION", StringType())) \
            .withColumn("age", get_column(src_df, "Age", ShortType())) \
            .withColumn("region_id", get_column(src_df, "region_id", IntegerType())) \
            .withColumn("opt_in", get_column(src_df, "Opt_In", StringType())) \
            .withColumn("patient_death_indicator", get_column(src_df, "PATIENT_DEATH_INDICATOR", StringType())) \
            .withColumn("patient_death_date", get_column(src_df, "PATIENT_DEATH_DATE", DateType())) \
            .withColumn("beneficiary_status", get_column(src_df, "BENEFICIARY_STATUS", StringType())) \
            .withColumn("bene_mdcr_stus_cd", get_column(src_df, "BENE_MDCR_STUS_CD", StringType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", StringType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .withColumn("region_name", get_column(src_df, "RegionName", StringType())) \
            .withColumn("program_id", get_column(src_df, "PROGRAM_ID", IntegerType())) \
            .withColumn("program_name", get_column(src_df, "PROGRAM_NAME", StringType())) \
            .withColumn("consent_from_date", get_column(src_df, "Consent_from_date", DateType())) \
            .withColumn("consent_to_date", get_column(src_df, "Consent_to_date", DateType())) \
            .withColumn("contact_status", get_column(src_df, "ContactStatus", StringType())) \
            .withColumn("previous_hrk", get_column(src_df, "PREVIOUS_HRK", IntegerType())) \
            .withColumn("ethnicity", get_column(src_df, "ETHNICITY", StringType())) \
            .withColumn("ethnicity_code", get_column(src_df, "ETHNICITY_CODE", StringType())) \
            .withColumn("client_id", get_column(src_df, "clientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("d_first_name", get_column(src_df, "DFirstName", StringType())) \
            .withColumn("d_last_name", get_column(src_df, "DLastName", StringType())) \
            .withColumn("d_member_id", get_column(src_df, "DMemberID", StringType())) \
            .select([
                "health_token_id",
                "health_record_key",
                "medicare",
                "dob",
                "ssn",
                "mothers_maiden_name",
                "street_address1",
                "street_address2",
                "city",
                "state",
                "zip",
                "phone_home",
                "phone_work",
                "phone_mobile",
                "email",
                "fax",
                "last_name",
                "first_name",
                "middle_initials",
                "sex",
                "marital_status",
                "nationality",
                "title",
                "birth_place",
                "primary_language",
                "secondary_language",
                "alias",
                "alert_desc",
                "race_description",
                "age",
                "region_id",
                "opt_in",
                "patient_death_indicator",
                "patient_death_date",
                "beneficiary_status",
                "bene_mdcr_stus_cd",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date",
                "region_name",
                "program_id",
                "program_name",
                "consent_from_date",
                "consent_to_date",
                "contact_status",
                "previous_hrk",
                "ethnicity",
                "ethnicity_code",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
                "d_first_name",
                "d_last_name",
                "d_member_id"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.beneficiary_details (
                health_token_id BINARY,
                health_record_key INTEGER,
                medicare STRING,
                dob DATE,
                ssn STRING,
                mothers_maiden_name STRING,
                street_address1 STRING,
                street_address2 STRING,
                city STRING,
                state STRING,
                zip STRING,
                phone_home STRING,
                phone_work STRING,
                phone_mobile STRING,
                email STRING,
                fax STRING,
                last_name STRING,
                first_name STRING,
                middle_initials STRING,
                sex STRING,
                marital_status STRING,
                nationality STRING,
                title STRING,
                birth_place STRING,
                primary_language STRING,
                secondary_language STRING,
                alias STRING,
                alert_desc STRING,
                race_description STRING,
                age SHORT,
                region_id INTEGER,
                opt_in STRING,
                patient_death_indicator STRING,
                patient_death_date DATE,
                beneficiary_status STRING,
                bene_mdcr_stus_cd STRING,
                created_by STRING,
                created_date TIMESTAMP,
                updated_by STRING,
                updated_date TIMESTAMP,
                region_name STRING,
                program_id INTEGER,
                program_name STRING,
                consent_from_date DATE,
                consent_to_date DATE,
                contact_status STRING,
                previous_hrk INTEGER,
                ethnicity STRING,
                ethnicity_code STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                d_first_name STRING,
                d_last_name STRING,
                d_member_id STRING
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/beneficiary-details'
        """)

        # write data into delta lake beneficiary_details table
        beneficiary_details.write.format("delta").mode("append").save(beneficiary_details_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
