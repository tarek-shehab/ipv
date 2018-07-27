import importlib
helpers = importlib.import_module('.helpers', 'models')

######################################################################
#
######################################################################
table  = 'assignment_assignor'
body   =   ('(app_id BIGINT,'
            'reel_no STRING,'
            'frame_no STRING,'
            'name STRING,'
            'execution_date STRING,'
            'address1 STRING,'
            'address2 STRING, '
            'PRIMARY KEY(app_id, reel_no, frame_no, name)) PARTITION BY HASH(app_id) PARTITIONS 16 STORED AS KUDU ')

body_ext =   ('(app_id BIGINT,'
            'reel_no STRING,'
            'frame_no STRING,'
            'name STRING,'
            'execution_date STRING,'
            'address1 STRING,'
            'address2 STRING) ')

model = helpers.tbl_model(table, [body, body_ext])
