import helpers
######################################################################
#
######################################################################
table  = 'a_assignor'
body   =   ('(app_id BIGINT,'
            'reel_no STRING,'
            'frame_no STRING,'
            'name STRING,'
            'execution_date STRING,'
            'address1 STRING,'
            'address2 STRING) ')

model = helpers.tbl_model(table, body)
