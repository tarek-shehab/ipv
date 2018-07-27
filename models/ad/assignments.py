import importlib
helpers = importlib.import_module('.helpers', 'models')

######################################################################
#
######################################################################
table  = 'assignment_main'
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
            'conv_text STRING, '
            'PRIMARY KEY(app_id)) PARTITION BY HASH(app_id) PARTITIONS 16 STORED AS KUDU ')

body_ext =   ('(app_id BIGINT,'
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

model = helpers.tbl_model(table, [body, body_ext])
