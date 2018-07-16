import helpers
######################################################################
#
######################################################################
table  = 'claims'

body   =   ('(app_id BIGINT,'
            'country STRING,'
            'doc_number STRING,'
            '`date` STRING) ')

model = helpers.tbl_model(table, body)

