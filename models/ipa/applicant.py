import helpers
######################################################################
#
######################################################################
table  = 'applicant'

body   =   ('(app_id BIGINT,'
            'residence STRING,'
            'nationality STRING,'
            'addr_city STRING,'
            'addr_country STRING,'
            'addr_state STRING,'
            'first_name STRING,'
            'last_name STRING) ')

model = helpers.tbl_model(table, body)

