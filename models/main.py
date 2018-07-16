import helpers
######################################################################
#
######################################################################
table  = 'main'
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

model = helpers.tbl_model(table, body)
