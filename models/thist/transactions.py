import importlib
helpers = importlib.import_module('.helpers', 'models')

######################################################################
#
######################################################################
table  = 'tr_history'
body   =   ('(app_id BIGINT,'
            'record_date STRING,'
            'code STRING,'
            'description STRING,'
            'PRIMARY KEY(app_id, record_date)) PARTITION BY HASH(app_id) PARTITIONS 64 STORED AS KUDU ')

body_ext =   ('(app_id BIGINT,'
            'record_date STRING,'
            'code STRING,'
            'description STRING) ')

model = helpers.tbl_model(table, [body, body_ext])
