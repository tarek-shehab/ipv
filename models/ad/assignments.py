import helpers
######################################################################
#
######################################################################
table  = 'assignments'
body   =   ('(app_id BIGINT,'
            'reel_no STRING,'
            'frame_no STRING,'
            'last_update STRING,'
            'purge_ind STRING,'
            'recorded STRING,'
            'corr_name STRING,'
            'corr_addr1 STRING,'
            'corr_addr2 STRING,'
            'corr_addr3 STRING,'
            'corr_addr4 STRING,'
            'conv_text STRING) ')

model = helpers.tbl_model(table, [body, None])
