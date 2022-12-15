import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_FACT_MEDICAL_CLAIMS_PATH'] = 's3a://delta-lake-tables/udw/fact_medical_claims.csv'
os.environ['FACT_MEDICAL_CLAIMS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/fact_medical_claims'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_fact_medical_claims_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        fact_medical_claims_src_file_path = os.environ.get("UDW_FACT_MEDICAL_CLAIMS_PATH", "")
        if not fact_medical_claims_src_file_path:
            exit_with_error(logger, f"fact_medical_claims source file path should be provided!")

        fact_medical_claims_delta_table_path = os.environ.get("FACT_MEDICAL_CLAIMS_DELTA_TABLE_PATH", "")
        if not fact_medical_claims_delta_table_path:
            exit_with_error(logger, f"fact_medical_claims delta table path should be provided!")

        # read fact_medical_claims source csv file from s3 bucket
        logger.warn(f"read fact_medical_claims source csv file from s3")
        src_df = spark.read.csv(fact_medical_claims_src_file_path, header=True)
        fact_medical_claims_df = src_df \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("claim_key", get_column(src_df, "ClaimKey", LongType())) \
            .withColumn("line_number", get_column(src_df, "LineNumber", IntegerType())) \
            .withColumn("claim_status", get_column(src_df, "ClaimStatus", StringType())) \
            .withColumn("claim_type", get_column(src_df, "ClaimType", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", IntegerType())) \
            .withColumn("pharmacy_flag", get_column(src_df, "PharmacyFlag", StringType())) \
            .withColumn("lob", get_column(src_df, "LOB", StringType())) \
            .withColumn("region", get_column(src_df, "Region", StringType())) \
            .withColumn("client", get_column(src_df, "Client", StringType())) \
            .withColumn("product", get_column(src_df, "Product", StringType())) \
            .withColumn("unit", get_column(src_df, "Unit", StringType())) \
            .withColumn("service_from_date", get_column(src_df, "ServiceFromDate", DateType())) \
            .withColumn("service_to_date", get_column(src_df, "ServiceToDate", DateType())) \
            .withColumn("admition_date", get_column(src_df, "AdmitionDate", DateType())) \
            .withColumn("discharge_date", get_column(src_df, "DischargeDate", DateType())) \
            .withColumn("paid_date", get_column(src_df, "PaidDate", DateType())) \
            .withColumn("bill_amount", get_column(src_df, "BillAmount", DecimalType())) \
            .withColumn("paid_amount", get_column(src_df, "PaidAmount", DecimalType())) \
            .withColumn("location_code", get_column(src_df, "LocationCode", StringType())) \
            .withColumn("bill_class_id", get_column(src_df, "BillClassID", IntegerType())) \
            .withColumn("diag1", get_column(src_df, "Diag1", StringType())) \
            .withColumn("diag_desc1", get_column(src_df, "DiagDesc1", StringType())) \
            .withColumn("diag_group_desc1", get_column(src_df, "DiagGroupDesc1", StringType())) \
            .withColumn("icd_version1", get_column(src_df, "ICDVersion1", StringType())) \
            .withColumn("diag2", get_column(src_df, "Diag2", StringType())) \
            .withColumn("diag_desc2", get_column(src_df, "DiagDesc2", StringType())) \
            .withColumn("diag_group_desc2", get_column(src_df, "DiagGroupDesc2", StringType())) \
            .withColumn("icd_version2", get_column(src_df, "ICDVersion2", StringType())) \
            .withColumn("diag3", get_column(src_df, "Diag3", StringType())) \
            .withColumn("diag_desc3", get_column(src_df, "DiagDesc3", StringType())) \
            .withColumn("diag_group_desc3", get_column(src_df, "DiagGroupDesc3", StringType())) \
            .withColumn("icd_version3", get_column(src_df, "ICDVersion3", StringType())) \
            .withColumn("diag4", get_column(src_df, "Diag4", StringType())) \
            .withColumn("diag_desc4", get_column(src_df, "DiagDesc4", StringType())) \
            .withColumn("diag_group_desc4", get_column(src_df, "DiagGroupDesc4", StringType())) \
            .withColumn("icd_version4", get_column(src_df, "ICDVersion4", StringType())) \
            .withColumn("drg_code", get_column(src_df, "DRG_Code", StringType())) \
            .withColumn("type", get_column(src_df, "Type", StringType())) \
            .withColumn("ms_drg_title", get_column(src_df, "MS-DRG", StringType())) \
            .withColumn("revenue_code", get_column(src_df, "RevenueCode", StringType())) \
            .withColumn("revenue_description", get_column(src_df, "RevenueDESCRIPTION", StringType())) \
            .withColumn("procedure_code", get_column(src_df, "ProcedureCode", StringType())) \
            .withColumn("code_type", get_column(src_df, "CodeType", StringType())) \
            .withColumn("description_code_desc", get_column(src_df, "DescriptioncodeDesc", StringType())) \
            .withColumn("code_modifier", get_column(src_df, "CodeModifier", StringType())) \
            .withColumn("pat_discharge", get_column(src_df, "Pat_Discharge", StringType())) \
            .withColumn("participating_flag", get_column(src_df, "ParticipatingFlag", StringType())) \
            .withColumn("rendering_provider_npi", get_column(src_df, "RenderingProviderNPI", StringType())) \
            .withColumn("rendering_provider_id", get_column(src_df, "RenderingProviderID", LongType())) \
            .withColumn("rendering_facility_npi", get_column(src_df, "RenderingFacilityNPI", StringType())) \
            .withColumn("rendering_facility_id", get_column(src_df, "RenderingFacilityID", LongType())) \
            .withColumn("referring_provider_npi", get_column(src_df, "ReferringProviderNPI", StringType())) \
            .withColumn("referring_provider_id", get_column(src_df, "ReferringProviderID", LongType())) \
            .withColumn("pcp", get_column(src_df, "PCP", StringType())) \
            .withColumn("pcp_name", get_column(src_df, "PCPName", StringType())) \
            .withColumn("tin", get_column(src_df, "TIN", StringType())) \
            .withColumn("practice_name", get_column(src_df, "PracticeName", StringType())) \
            .withColumn("main_category", get_column(src_df, "MainCategory", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("source_id", get_column(src_df, "SourceID", IntegerType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "id",
                "claim_key",
                "line_number",
                "claim_status",
                "claim_type",
                "health_token_id",
                "health_record_key",
                "pharmacy_flag",
                "lob",
                "region",
                "client",
                "product",
                "unit",
                "service_from_date",
                "service_to_date",
                "admition_date",
                "discharge_date",
                "paid_date",
                "bill_amount",
                "paid_amount",
                "location_code",
                "bill_class_id",
                "diag1",
                "diag_desc1",
                "diag_group_desc1",
                "icd_version1",
                "diag2",
                "diag_desc2",
                "diag_group_desc2",
                "icd_version2",
                "diag3",
                "diag_desc3",
                "diag_group_desc3",
                "icd_version3",
                "diag4",
                "diag_desc4",
                "diag_group_desc4",
                "icd_version4",
                "drg_code",
                "type",
                "ms_drg_title",
                "revenue_code",
                "revenue_description",
                "procedure_code",
                "code_type",
                "description_code_desc",
                "code_modifier",
                "pat_discharge",
                "participating_flag",
                "rendering_provider_npi",
                "rendering_provider_id",
                "rendering_facility_npi",
                "rendering_facility_id",
                "referring_provider_npi",
                "referring_provider_id",
                "pcp",
                "pcp_name",
                "tin",
                "practice_name",
                "main_category",
                "client_id",
                "client_name",
                "source_id",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.fact_medical_claims (
                id INTEGER,
                claim_key LONG,
                line_number INTEGER,
                claim_status STRING,
                claim_type STRING,
                health_token_id BINARY,
                health_record_key INTEGER,
                pharmacy_flag STRING,
                lob STRING,
                region STRING,
                client STRING,
                product STRING,
                unit STRING,
                service_from_date DATE,
                service_to_date DATE,
                admition_date DATE,
                discharge_date DATE,
                paid_date DATE,
                bill_amount DECIMAL,
                paid_amount DECIMAL,
                location_code STRING,
                bill_class_id INTEGER,
                diag1 STRING,
                diag_desc1 STRING,
                diag_group_desc1 STRING,
                icd_version1 STRING,
                diag2 STRING,
                diag_desc2 STRING,
                diag_group_desc2 STRING,
                icd_version2 STRING,
                diag3 STRING,
                diag_desc3 STRING,
                diag_group_desc3 STRING,
                icd_version3 STRING,
                diag4 STRING,
                diag_desc4 STRING,
                diag_group_desc4 STRING,
                icd_version4 STRING,
                drg_code STRING,
                type STRING,
                ms_drg_title STRING,
                revenue_code STRING,
                revenue_description STRING,
                procedure_code STRING,
                code_type STRING,
                description_code_desc STRING,
                code_modifier STRING,
                pat_discharge STRING,
                participating_flag STRING,
                rendering_provider_npi STRING,
                rendering_provider_id LONG,
                rendering_facility_npi STRING,
                rendering_facility_id LONG,
                referring_provider_npi STRING,
                referring_provider_id LONG,
                pcp STRING,
                pcp_name STRING,
                tin STRING,
                practice_name STRING,
                main_category STRING,
                client_id INTEGER,
                client_name STRING,
                source_id INTEGER,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/fact_medical_claims'
        """)

        # write data into delta lake fact_medical_claims table
        fact_medical_claims_df.write.format("delta").mode("append").save(fact_medical_claims_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
