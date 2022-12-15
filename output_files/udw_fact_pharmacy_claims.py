import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_FACT_PHARMACY_CLAIMS_PATH'] = 's3a://delta-lake-tables/udw/fact_pharmacy_claims.csv'
os.environ['FACT_PHARMACY_CLAIMS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/fact_pharmacy_claims'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_fact_pharmacy_claims_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        fact_pharmacy_claims_src_file_path = os.environ.get("UDW_FACT_PHARMACY_CLAIMS_PATH", "")
        if not fact_pharmacy_claims_src_file_path:
            exit_with_error(logger, f"fact_pharmacy_claims source file path should be provided!")

        fact_pharmacy_claims_delta_table_path = os.environ.get("FACT_PHARMACY_CLAIMS_DELTA_TABLE_PATH", "")
        if not fact_pharmacy_claims_delta_table_path:
            exit_with_error(logger, f"fact_pharmacy_claims delta table path should be provided!")

        # read fact_pharmacy_claims source csv file from s3 bucket
        logger.warn(f"read fact_pharmacy_claims source csv file from s3")
        src_df = spark.read.csv(fact_pharmacy_claims_src_file_path, header=True)
        fact_pharmacy_claims_df = src_df \
            .withColumn("rx_number", get_column(src_df, "RxNumber", StringType())) \
            .withColumn("nabp", get_column(src_df, "NABP", StringType())) \
            .withColumn("pharmacy_name", get_column(src_df, "PharmacyName", StringType())) \
            .withColumn("mail_order_flag", get_column(src_df, "MailOrderFlag", StringType())) \
            .withColumn("prescriber_npi", get_column(src_df, "PrescriberNPI", StringType())) \
            .withColumn("prescriber_id", get_column(src_df, "PrescriberID", LongType())) \
            .withColumn("dispense_date", get_column(src_df, "DispenseDate", StringType())) \
            .withColumn("paid_date", get_column(src_df, "PaidDate", StringType())) \
            .withColumn("ndc_code", get_column(src_df, "NDCCode", StringType())) \
            .withColumn("gpi", get_column(src_df, "GPI", StringType())) \
            .withColumn("ahfs", get_column(src_df, "AHFS", StringType())) \
            .withColumn("supply", get_column(src_df, "Supply", StringType())) \
            .withColumn("qty", get_column(src_df, "Qty", StringType())) \
            .withColumn("copay", get_column(src_df, "Copay", StringType())) \
            .withColumn("paid_amount", get_column(src_df, "PaidAmount", StringType())) \
            .withColumn("rx_claim_id", get_column(src_df, "RxClaimID", StringType())) \
            .withColumn("policy_number", get_column(src_df, "PolicyNumber", StringType())) \
            .withColumn("plan_id", get_column(src_df, "PlanID", StringType())) \
            .withColumn("dispens_as_written", get_column(src_df, "DispensAsWritten", StringType())) \
            .withColumn("cms_claim_id", get_column(src_df, "CMSClaimID", IntegerType())) \
            .withColumn("claim_status", get_column(src_df, "ClaimStatus", StringType())) \
            .withColumn("comments", get_column(src_df, "Comments", StringType())) \
            .withColumn("billed_date", get_column(src_df, "BilledDate", StringType())) \
            .withColumn("billed_amount", get_column(src_df, "BilledAmount", DecimalType())) \
            .withColumn("health_plan_id", get_column(src_df, "HealthPlanID", StringType())) \
            .withColumn("brand_gen", get_column(src_df, "BrandGEN", StringType())) \
            .withColumn("drug_name", get_column(src_df, "DrugName", StringType())) \
            .withColumn("brand_or_generic_name", get_column(src_df, "BrandOrGenericName", StringType())) \
            .withColumn("ndc_strength", get_column(src_df, "NDCStrength", StringType())) \
            .withColumn("market_status", get_column(src_df, "MarketStatus", StringType())) \
            .withColumn("line_of_business", get_column(src_df, "LineOfBusiness", StringType())) \
            .withColumn("strength", get_column(src_df, "Strength", StringType())) \
            .withColumn("region", get_column(src_df, "Region", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("practice_name", get_column(src_df, "PracticeName", StringType())) \
            .withColumn("source_id", get_column(src_df, "SourceID", IntegerType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "rx_number",
                "nabp",
                "pharmacy_name",
                "mail_order_flag",
                "prescriber_npi",
                "prescriber_id",
                "dispense_date",
                "paid_date",
                "ndc_code",
                "gpi",
                "ahfs",
                "supply",
                "qty",
                "copay",
                "paid_amount",
                "rx_claim_id",
                "policy_number",
                "plan_id",
                "dispens_as_written",
                "cms_claim_id",
                "claim_status",
                "comments",
                "billed_date",
                "billed_amount",
                "health_plan_id",
                "brand_gen",
                "drug_name",
                "brand_or_generic_name",
                "ndc_strength",
                "market_status",
                "line_of_business",
                "strength",
                "region",
                "client_id",
                "client_name",
                "health_record_key",
                "health_token_id",
                "practice_name",
                "source_id",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.fact_pharmacy_claims (
                rx_number STRING,
                nabp STRING,
                pharmacy_name STRING,
                mail_order_flag STRING,
                prescriber_npi STRING,
                prescriber_id LONG,
                dispense_date STRING,
                paid_date STRING,
                ndc_code STRING,
                gpi STRING,
                ahfs STRING,
                supply STRING,
                qty STRING,
                copay STRING,
                paid_amount STRING,
                rx_claim_id STRING,
                policy_number STRING,
                plan_id STRING,
                dispens_as_written STRING,
                cms_claim_id INTEGER,
                claim_status STRING,
                comments STRING,
                billed_date STRING,
                billed_amount DECIMAL,
                health_plan_id STRING,
                brand_gen STRING,
                drug_name STRING,
                brand_or_generic_name STRING,
                ndc_strength STRING,
                market_status STRING,
                line_of_business STRING,
                strength STRING,
                region STRING,
                client_id INTEGER,
                client_name STRING,
                health_record_key INTEGER,
                health_token_id BINARY,
                practice_name STRING,
                source_id INTEGER,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/fact_pharmacy_claims'
        """)

        # write data into delta lake fact_pharmacy_claims table
        fact_pharmacy_claims_df.write.format("delta").mode("append").save(fact_pharmacy_claims_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
