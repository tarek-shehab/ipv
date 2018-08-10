import importlib
helpers = importlib.import_module('.helpers', 'models')

######################################################################
#
######################################################################
table  = 'old_application_claims'

body   =   ('(app_id BIGINT,'
            'country STRING,'
            'doc_number STRING,'
            '`date` STRING) ')

model = helpers.tbl_model(table, [body, None])

