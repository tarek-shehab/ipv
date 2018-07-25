import helpers
######################################################################
#
######################################################################
table  = 'assignee'

body   =   ('(app_id BIGINT,'
            'residence STRING,'
            'nationality STRING,'
            'addr_city STRING,'
            'addr_country STRING,'
            'addr_state STRING,'
            'first_name STRING,'
            'last_name STRING,'
            'asn_role STRING ) ')

model = helpers.tbl_model(table, body)
