import helpers
######################################################################
#
######################################################################
table  = 'attorney'
body   =   ('(last_name STRING,'
            'first_name STRING,'
            'middle_name STRING,'
            'name_suffix STRING,'
            'company STRING,'
            'address1 STRING,'
            'address2 STRING,'
            'address3 STRING,'
            'city STRING,'
            'state STRING,'
            'country STRING,'
            'postal_code STRING,'
            'phone_no STRING,'
            'reg_no STRING,'
            'indication STRING,'
            'gov_empl_status STRING) ')

model = helpers.tbl_model(table, body)
