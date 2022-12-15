import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_NGS_THERAPY_CLINICAL_TRIALS_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/NGS_THERAPY_CLINICAL_TRIALS.csv"
# export NGS_THERAPY_CLINICAL_TRIALS_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/NGS_THERAPY_CLINICAL_TRIALS"


def main():
    app_name = "udw_ngs_therapy_clinical_trials_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        ngs_therapy_clinical_trials_src_file_path = os.environ.get("UDW_NGS_THERAPY_CLINICAL_TRIALS_FILE_PATH", "")
        if not ngs_therapy_clinical_trials_src_file_path:
            exit_with_error(logger, f"ngs_therapy_clinical_trials source file path should be provided!")

        ngs_therapy_clinical_trials_delta_table_path = os.environ.get("NGS_THERAPY_CLINICAL_TRIALS_DELTA_TABLE_PATH", "")
        if not ngs_therapy_clinical_trials_delta_table_path:
            exit_with_error(logger, f"ngs_therapy_clinical_trials delta table path should be provided!")

        # read ngs_therapy_clinical_trials source csv file from s3 bucket
        logger.warn(f"read ngs_therapy_clinical_trials source csv file from s3")
        src_df = spark.read.csv(ngs_therapy_clinical_trials_src_file_path, header=True)
        
        ngs_therapy_clinical_trials_df = src_df \
                .withColumn('therapy_id ', get_column(src_df,'therapy_id ', IntegerType()))\
                .withColumn('health_record_key ', get_column(src_df,'healthrecordkey ', IntegerType()))\
                .withColumn('order_id ', get_column(src_df,'order_id '))\
                .withColumn('therapy_details_therapy ', get_column(src_df,'therapy_details_therapy '))\
                .withColumn('therapy_details_therapy_description ', get_column(src_df,'therapy_details_therapy_description '))\
                .withColumn('indication_fda_approved ', get_column(src_df,'indication_fda_approved '))\
                .withColumn('indication_nccn_recommended ', get_column(src_df,'indication_nccn_recommended '))\
                .withColumn('indication_fda_uncertain_benefit ', get_column(src_df,'indication_fda_uncertain_benefit '))\
                .withColumn('indication_nccn_uncertain_benefit ', get_column(src_df,'indication_nccn_uncertain_benefit '))\
                .withColumn('indication_expanded_access ', get_column(src_df,'indication_expanded_access '))\
                .withColumn('indication_benefit_statement ', get_column(src_df,'indication_benefit_statement '))\
                .withColumn('indication_uncertain_benefit_statement ', get_column(src_df,'indication_uncertain_benefit_statement '))\
                .withColumn('clinical_significance_amp_cap_asco_tier ', get_column(src_df,'clinical_significance_amp_cap_asco_tier '))\
                .withColumn('clinical_significance_evidence ', get_column(src_df,'clinical_significance_evidence '))\
                .withColumn('clinical_trials_trial_id ', get_column(src_df,'clinical_trials_trial_id '))\
                .withColumn('clinical_trials_name ', get_column(src_df,'clinical_trials_name '))\
                .withColumn('clinical_trials_phase ', get_column(src_df,'clinical_trials_phase '))\
                .withColumn('clinical_trials_location ', get_column(src_df,'clinical_trials_location '))\
                .withColumn('therapy_details_report_section ', get_column(src_df,'therapy_details_report_section '))\
                .withColumn('marker_profiles_report_sections ', get_column(src_df,'marker_profiles_report_sections '))\
                .withColumn('therapy_considerations_therapy ', get_column(src_df,'therapy_considerations_therapy '))\
                .withColumn('therapy_considerations_indications ', get_column(src_df,'therapy_considerations_indications '))\
                .withColumn('therapy_considerations_sources ', get_column(src_df,'therapy_considerations_sources '))\
                .withColumn('therapy_considerations_other_tumor_type ', get_column(src_df,'therapy_considerations_other_tumor_type '))\
                .withColumn('therapy_considerations_report_section ', get_column(src_df,'therapy_considerations_report_section '))\
                .withColumn('marker ', get_column(src_df,'marker '))\
                .withColumn('client_id ', get_column(src_df,'ClientID ', IntegerType()))\
                .withColumn('client_name ', get_column(src_df,'ClientName '))\
                .withColumn('health_token_id', get_column(src_df,'HealthTokenID', BinaryType())) \
                .select([
                'therapy_id ',
                'health_record_key ',
                'order_id ',
                'therapy_details_therapy ',
                'therapy_details_therapy_description ',
                'indication_fda_approved ',
                'indication_nccn_recommended ',
                'indication_fda_uncertain_benefit ',
                'indication_nccn_uncertain_benefit ',
                'indication_expanded_access ',
                'indication_benefit_statement ',
                'indication_uncertain_benefit_statement ',
                'clinical_significance_amp_cap_asco_tier ',
                'clinical_significance_evidence ',
                'clinical_trials_trial_id ',
                'clinical_trials_name ',
                'clinical_trials_phase ',
                'clinical_trials_location ',
                'therapy_details_report_section ',
                'marker_profiles_report_sections ',
                'therapy_considerations_therapy ',
                'therapy_considerations_indications ',
                'therapy_considerations_sources ',
                'therapy_considerations_other_tumor_type ',
                'therapy_considerations_report_section ',
                'marker ',
                'client_id ',
                'client_name ',
                'health_token_id',
            ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.ngs_therapy_clinical_trials(
                therapy_id INTEGER,
                health_record_key INTEGER,
                order_id STRING,
                therapy_details_therapy STRING,
                therapy_details_therapy_description STRING,
                indication_fda_approved STRING,
                indication_nccn_recommended STRING,
                indication_fda_uncertain_benefit STRING,
                indication_nccn_uncertain_benefit STRING,
                indication_expanded_access STRING,
                indication_benefit_statement STRING,
                indication_uncertain_benefit_statement STRING,
                clinical_significance_amp_cap_asco_tier STRING,
                clinical_significance_evidence STRING,
                clinical_trials_trial_id STRING,
                clinical_trials_name STRING,
                clinical_trials_phase STRING,
                clinical_trials_location STRING,
                therapy_details_report_section STRING,
                marker_profiles_report_sections STRING,
                therapy_considerations_therapy STRING,
                therapy_considerations_indications STRING,
                therapy_considerations_sources STRING,
                therapy_considerations_other_tumor_type STRING,
                therapy_considerations_report_section STRING,
                marker STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY
                )
                USING delta
                PARTITIONED BY (client_id)
                LOCATION ''
            """)

        # write data into delta lake ngs_therapy_clinical_trials table
        ngs_therapy_clinical_trials_df.write.format("delta").mode("append").save(ngs_therapy_clinical_trials_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()