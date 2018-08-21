import importlib
helpers = importlib.import_module('.helpers', 'models')

######################################################################
#
######################################################################
table  = 'old_grant_provisional'

body   =   ('(app_id BIGINT,'
            'country STRING,'
            'doc_number STRING,'
            'kind STRING,'
            '`date` STRING) ')

model = helpers.tbl_model(table, [body, None])
