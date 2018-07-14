
ext_header = 'CREATE EXTERNAL TABLE IF NOT EXISTS `db_ext`.`main` '
int_header = 'CREATE TABLE IF NOT EXISTS `db_int`.`main` '
ext_body = ('(proc_date STRING,'
            'app_id BIGINT,'
            'pub_ref_country STRING,'
            'pub_ref_doc_number STRING,'
            'pub_ref_kind STRING,'
            'pub_ref_date STRING,'
            'app_ref_country STRING,'
            'app_ref_date STRING,'
            'us_app_ser_code STRING,'
            'us_iss_on_cnt_prc_app STRING,'
            'rule_47 STRING,'
            'us_term_ext STRING,'
            'len_of_grant STRING,'
            'inv_title STRING,'
            'nbr_of_claims BIGINT,'
            'us_exempl_claim STRING,'
            'us_botan_var STRING,'
            'us_prov_app_country STRING,'
            'us_prov_app_doc_number STRING,'
            'us_prov_app_kind STRING,'
            'us_prov_app_date STRING,'
            'rel_pub_country STRING,'
            'rel_pub_doc_number STRING,'
            'rel_pub_kind STRING,'
            'rel_pub_date STRING) '
        'PARTITIONED BY (day STRING, hour STRING) '
        'ROW FORMAT DELIMITED FIELDS TERMINATED BY \'\\t\'') % (params['instance'],params['raw_log']), 

    to_extract = [".//application-reference/document-id/doc-number",
                  ".//publication-reference/document-id/country",
                  ".//publication-reference/document-id/doc-number",
                  ".//publication-reference/document-id/kind",
                  ".//publication-reference/document-id/date",
                  ".//application-reference/document-id/country",
                  ".//application-reference/document-id/date",
                  ".//us-application-series-code",
                  ".//us-issued-on-continued-prosecution-application",
                  ".//rule-47-flag",
                  ".//us-term-extension",
                  ".//length-of-grant",
                  ".//invention-title",
                  ".//number-of-claims",
                  ".//us-exemplary-claim",
                  ".//us-botanic/variety",
                  ".//us-provisional-application/document-id/country",
                  ".//us-provisional-application/document-id/doc-number",
                  ".//us-provisional-application/document-id/kind",
                  ".//us-provisional-application/document-id/date",
                  ".//related-publication/document-id/country",
                  ".//related-publication/document-id/doc-number",
                  ".//related-publication/document-id/kind",
                  ".//related-publication/document-id/date"
                 ]
