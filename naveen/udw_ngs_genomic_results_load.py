import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_NGS_GENOMIC_RESULTS_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/NGS_GENOMIC_RESULTS.csv"
# export NGS_GENOMIC_RESULTS_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/NGS_GENOMIC_RESULTS"


def main():
    app_name = "udw_ngs_genomic_results_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        ngs_genomic_results_src_file_path = os.environ.get("UDW_NGS_GENOMIC_RESULTS_FILE_PATH", "")
        if not ngs_genomic_results_src_file_path:
            exit_with_error(logger, f"ngs_genomic_results source file path should be provided!")

        ngs_genomic_results_delta_table_path = os.environ.get("NGS_GENOMIC_RESULTS_DELTA_TABLE_PATH", "")
        if not ngs_genomic_results_delta_table_path:
            exit_with_error(logger, f"ngs_genomic_results delta table path should be provided!")

        # read ngs_genomic_results source csv file from s3 bucket
        logger.warn(f"read ngs_genomic_results source csv file from s3")
        src_df = spark.read.csv(ngs_genomic_results_src_file_path, header=True)
        
        ngs_genomic_results_df = src_df \
            .withColumn('genomin_id ', get_column(src_df,'Genomin_id', IntegerType()))\
            .withColumn('health_record_key ', get_column(src_df,'HealthRecordKey', IntegerType()))\
            .withColumn('order_id ', get_column(src_df,'order_id'))\
            .withColumn('name ', get_column(src_df,'name'))\
            .withColumn('go_name ', get_column(src_df,'go_name'))\
            .withColumn('test ', get_column(src_df,'test'))\
            .withColumn('marker ', get_column(src_df,'marker'))\
            .withColumn('result_type ', get_column(src_df,'result_type'))\
            .withColumn('gene_type ', get_column(src_df,'gene_type'))\
            .withColumn('variant_type ', get_column(src_df,'variant_type'))\
            .withColumn('snv_indel_details_gene ', get_column(src_df,'snv_indel_details_gene'))\
            .withColumn('snv_indel_details_alteration ', get_column(src_df,'snv_indel_details_alteration'))\
            .withColumn('snv_indel_details_location ', get_column(src_df,'snv_indel_details_location'))\
            .withColumn('snv_indel_details_vaf ', get_column(src_df,'snv_indel_details_vaf'))\
            .withColumn('snv_indel_details_clinvar ', get_column(src_df,'snv_indel_details_clinvar'))\
            .withColumn('snv_indel_details_transcript_id ', get_column(src_df,'snv_indel_details_transcript_id'))\
            .withColumn('snv_indel_details_type ', get_column(src_df,'snv_indel_details_type'))\
            .withColumn('snv_indel_details_pathway ', get_column(src_df,'snv_indel_details_pathway'))\
            .withColumn('snv_indel_details_in_del_type ', get_column(src_df,'snv_indel_details_in_del_type'))\
            .withColumn('snv_indel_details_chromosome ', get_column(src_df,'snv_indel_details_chromosome'))\
            .withColumn('snv_indel_details_exon ', get_column(src_df,'snv_indel_details_exon'))\
            .withColumn('snv_indel_details_codon ', get_column(src_df,'snv_indel_details_codon'))\
            .withColumn('snv_indel_details_c_dot ', get_column(src_df,'snv_indel_details_c_dot'))\
            .withColumn('snv_indel_details_p_dot ', get_column(src_df,'snv_indel_details_p_dot'))\
            .withColumn('snv_indel_details_description ', get_column(src_df,'snv_indel_details_description'))\
            .withColumn('cnv_details_alteration ', get_column(src_df,'cnv_details_alteration'))\
            .withColumn('cnv_details_location ', get_column(src_df,'cnv_details_location'))\
            .withColumn('cnv_details_fold_change ', get_column(src_df,'cnv_details_fold_change'))\
            .withColumn('cnv_details_transcript_id ', get_column(src_df,'cnv_details_transcript_id'))\
            .withColumn('cnv_details_pathway ', get_column(src_df,'cnv_details_pathway'))\
            .withColumn('cnv_details_gene ', get_column(src_df,'cnv_details_gene'))\
            .withColumn('cnv_details_description ', get_column(src_df,'cnv_details_description'))\
            .withColumn('fusion_details_alteration ', get_column(src_df,'fusion_details_alteration'))\
            .withColumn('fusion_details_breakpoint ', get_column(src_df,'fusion_details_breakpoint'))\
            .withColumn('fusion_details_pathway ', get_column(src_df,'fusion_details_pathway'))\
            .withColumn('fusion_details_gene_description ', get_column(src_df,'fusion_details_gene_description'))\
            .withColumn('fusion_details_description ', get_column(src_df,'fusion_details_description'))\
            .withColumn('marker_profiles_name ', get_column(src_df,'marker_profiles_name'))\
            .withColumn('marker_profiles_components ', get_column(src_df,'marker_profiles_components'))\
            .withColumn('marker_profiles_report_sections ', get_column(src_df,'marker_profiles_report_sections'))\
            .withColumn('in_del_type ', get_column(src_df,'in_del_type'))\
            .withColumn('vaf ', get_column(src_df,'vaf'))\
            .withColumn('chromosome ', get_column(src_df,'chromosome'))\
            .withColumn('exon ', get_column(src_df,'exon', IntegerType()))\
            .withColumn('codon ', get_column(src_df,'codon', IntegerType()))\
            .withColumn('c_dot ', get_column(src_df,'c_dot'))\
            .withColumn('p_dot ', get_column(src_df,'p_dot'))\
            .withColumn('sift_class ', get_column(src_df,'sift_class'))\
            .withColumn('polyphen_class ', get_column(src_df,'polyphen_class'))\
            .withColumn('protein_family_description ', get_column(src_df,'protein_family_description'))\
            .withColumn('cnv_details_cnv_call ', get_column(src_df,'cnv_details_cnv_call'))\
            .withColumn('fusion_details_donor_gene ', get_column(src_df,'fusion_details_donor_gene'))\
            .withColumn('fusion_details_donor_exon ', get_column(src_df,'fusion_details_donor_exon'))\
            .withColumn('fusion_details_acceptor_gene ', get_column(src_df,'fusion_details_acceptor_gene'))\
            .withColumn('fusion_details_acceptor_exon ', get_column(src_df,'fusion_details_acceptor_exon'))\
            .withColumn('clinical_trials_trial_id ', get_column(src_df,'clinical_trials_trial_id'))\
            .withColumn('clinical_trials_name ', get_column(src_df,'clinical_trials_name'))\
            .withColumn('clinical_trials_phase ', get_column(src_df,'clinical_trials_phase'))\
            .withColumn('clinical_trials_location ', get_column(src_df,'clinical_trials_location'))\
            .withColumn('clinical_trials_distance_description ', get_column(src_df,'clinical_trials_distance_description'))\
            .withColumn('client_id ', get_column(src_df,'ClientID', IntegerType()))\
            .withColumn('client_name ', get_column(src_df,'ClientName'))\
            .withColumn('health_token_id ', get_column(src_df,'HealthTokenID', BinaryType()))\
            .select([
                'genomin_id ',
                'health_record_key ',
                'order_id ',
                'name ',
                'go_name ',
                'test ',
                'marker ',
                'result_type ',
                'gene_type ',
                'variant_type ',
                'snv_indel_details_gene ',
                'snv_indel_details_alteration ',
                'snv_indel_details_location ',
                'snv_indel_details_vaf ',
                'snv_indel_details_clinvar ',
                'snv_indel_details_transcript_id ',
                'snv_indel_details_type ',
                'snv_indel_details_pathway ',
                'snv_indel_details_in_del_type ',
                'snv_indel_details_chromosome ',
                'snv_indel_details_exon ',
                'snv_indel_details_codon ',
                'snv_indel_details_c_dot ',
                'snv_indel_details_p_dot ',
                'snv_indel_details_description ',
                'cnv_details_alteration ',
                'cnv_details_location ',
                'cnv_details_fold_change ',
                'cnv_details_transcript_id ',
                'cnv_details_pathway ',
                'cnv_details_gene ',
                'cnv_details_description ',
                'fusion_details_alteration ',
                'fusion_details_breakpoint ',
                'fusion_details_pathway ',
                'fusion_details_gene_description ',
                'fusion_details_description ',
                'marker_profiles_name ',
                'marker_profiles_components ',
                'marker_profiles_report_sections ',
                'in_del_type ',
                'vaf ',
                'chromosome ',
                'exon ',
                'codon ',
                'c_dot ',
                'p_dot ',
                'sift_class ',
                'polyphen_class ',
                'protein_family_description ',
                'cnv_details_cnv_call ',
                'fusion_details_donor_gene ',
                'fusion_details_donor_exon ',
                'fusion_details_acceptor_gene ',
                'fusion_details_acceptor_exon ',
                'clinical_trials_trial_id ',
                'clinical_trials_name ',
                'clinical_trials_phase ',
                'clinical_trials_location ',
                'clinical_trials_distance_description ',
                'client_id ',
                'client_name ',
                'health_token_id ',
                ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.ngs_genomic_results(
                genomin_id INTEGER,
                health_record_key INTEGER,
                order_id STRING,
                name STRING,
                go_name STRING,
                test STRING,
                marker STRING,
                result_type STRING,
                gene_type STRING,
                variant_type STRING,
                snv_indel_details_gene STRING,
                snv_indel_details_alteration STRING,
                snv_indel_details_location STRING,
                snv_indel_details_vaf STRING,
                snv_indel_details_clinvar STRING,
                snv_indel_details_transcript_id STRING,
                snv_indel_details_type STRING,
                snv_indel_details_pathway STRING,
                snv_indel_details_in_del_type STRING,
                snv_indel_details_chromosome STRING,
                snv_indel_details_exon STRING,
                snv_indel_details_codon STRING,
                snv_indel_details_c_dot STRING,
                snv_indel_details_p_dot STRING,
                snv_indel_details_description STRING,
                cnv_details_alteration STRING,
                cnv_details_location STRING,
                cnv_details_fold_change STRING,
                cnv_details_transcript_id STRING,
                cnv_details_pathway STRING,
                cnv_details_gene STRING,
                cnv_details_description STRING,
                fusion_details_alteration STRING,
                fusion_details_breakpoint STRING,
                fusion_details_pathway STRING,
                fusion_details_gene_description STRING,
                fusion_details_description STRING,
                marker_profiles_name STRING,
                marker_profiles_components STRING,
                marker_profiles_report_sections STRING,
                in_del_type STRING,
                vaf STRING,
                chromosome STRING,
                exon INTEGER,
                codon INTEGER,
                c_dot STRING,
                p_dot STRING,
                sift_class STRING,
                polyphen_class STRING,
                protein_family_description STRING,
                cnv_details_cnv_call STRING,
                fusion_details_donor_gene STRING,
                fusion_details_donor_exon STRING,
                fusion_details_acceptor_gene STRING,
                fusion_details_acceptor_exon STRING,
                clinical_trials_trial_id STRING,
                clinical_trials_name STRING,
                clinical_trials_phase INTEGER,
                clinical_trials_location STRING,
                clinical_trials_distance_description STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY
                        )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://sureshn-sandbox-landing-bucket/delta-tables/udw/practice'
        """)

        # write data into delta lake practice table
        ngs_genomic_results_df.write.format("delta").mode("append").save(ngs_genomic_results_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()