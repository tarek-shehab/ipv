import helpers
######################################################################
#
######################################################################
table  = 'a_assignee'
body   =   ('(app_id BIGINT,'
            'reel_no STRING,'
            'frame_no STRING,'
            'name STRING,'
            'city STRING,'
            'state STRING,'
            'country STRING,'
            'postcode STRING,'
            'address1 STRING,'
            'address2 STRING) ')

model = helpers.tbl_model(table, body)
