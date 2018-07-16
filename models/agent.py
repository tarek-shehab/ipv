import helpers
######################################################################
#
######################################################################
table  = 'agent'

body   =   ('(`app_id` BIGINT,'
             '`agent_type` STRING,'
             '`org_name` STRING,'
             '`country` STRING,'
             '`first_name` STRING,'
             '`last_name` STRING) ')

model = helpers.tbl_model(table, body)

