######################################################################
#
######################################################################
ext_db = 'ipv_ext'
int_db = 'ipv_db'
table  = 'main'
header = 'CREATE TABLE IF NOT EXISTS'
body   =   ('(app_id BIGINT,'
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
            'rel_pub_date STRING) ')

def get_int_schema():
    schema = ('%s `%s`.`%s` '
              '%s'
              'PARTITIONED BY (proc_date STRING) '
              'STORED AS PARQUET ') % (header, int_db, table, body)
    return schema

def get_ext_schema():
    schema = ('%s `%s`.`%s` '
              '%s'
              'ROW FORMAT DELIMITED FIELDS TERMINATED BY \'\\t\'') % (header, ext_db, table, body)
    return schema