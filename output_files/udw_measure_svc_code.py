import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_MEASURE_SVC_CODE_PATH'] = 's3a://delta-lake-tables/udw/measure_svc_code.csv'
os.environ['MEASURE_SVC_CODE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/measure_svc_code'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_measure_svc_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_svc_code_src_file_path = os.environ.get("UDW_MEASURE_SVC_CODE_PATH", "")
        if not measure_svc_code_src_file_path:
            exit_with_error(logger, f"measure_svc_code source file path should be provided!")

        measure_svc_code_delta_table_path = os.environ.get("MEASURE_SVC_CODE_DELTA_TABLE_PATH", "")
        if not measure_svc_code_delta_table_path:
            exit_with_error(logger, f"measure_svc_code delta table path should be provided!")

        # read measure_svc_code source csv file from s3 bucket
        logger.warn(f"read measure_svc_code source csv file from s3")
        src_df = spark.read.csv(measure_svc_code_src_file_path, header=True)
        measure_svc_code_df = src_df \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("svc_code", get_column(src_df, "SVCCODE", StringType())) \
            .withColumn("svc_descr", get_column(src_df, "SVCDESCR", StringType())) \
            .withColumn("service_from_date", get_column(src_df, "SERVICEFROMDATE", DateType())) \
            .withColumn("service_to_date", get_column(src_df, "SERVICETODATE", DateType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("dob", get_column(src_df, "DOB", DateType())) \
            .withColumn("claim_key", get_column(src_df, "CLAIMKEY", IntegerType())) \
            .withColumn("bill_class_key", get_column(src_df, "_billclasskey", StringType())) \
            .withColumn("modifier", get_column(src_df, "Modifier", StringType())) \
            .withColumn("cpt_modifier", get_column(src_df, "CPT_Modifier", StringType())) \
            .withColumn("rendering_npi", get_column(src_df, "Rendering_NPI", StringType())) \
            .withColumn("observation_code", get_column(src_df, "OBSERVATIONCODE", StringType())) \
            .withColumn("observation_code_system", get_column(src_df, "OBSERVATIONCODESYSTEM", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("rendering_provider_id", get_column(src_df, "RenderingProviderID", LongType())) \
            .select([
                "health_record_key",
                "svc_code",
                "svc_descr",
                "service_from_date",
                "service_to_date",
                "hie_source_code",
                "dob",
                "claim_key",
                "bill_class_key",
                "modifier",
                "cpt_modifier",
                "rendering_npi",
                "observation_code",
                "observation_code_system",
                "client_id",
                "client_name",
                "health_token_id",
                "target_created_date",
                "hash_bytes_sha",
                "rendering_provider_id",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_svc_code (
                health_record_key INTEGER,
                svc_code STRING,
                svc_descr STRING,
                service_from_date DATE,
                service_to_date DATE,
                hie_source_code STRING,
                dob DATE,
                claim_key INTEGER,
                bill_class_key STRING,
                modifier STRING,
                cpt_modifier STRING,
                rendering_npi STRING,
                observation_code STRING,
                observation_code_system STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                rendering_provider_id LONG
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/measure_svc_code'
        """)

        # write data into delta lake practice table
        measure_svc_code_df.write.format("delta").mode("append").save(measure_svc_code_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
