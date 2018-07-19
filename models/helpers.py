######################################################################
#
######################################################################
class tbl_model():

    def __init__(self, table, body):
        self.ext_db = 'ipv_ext'
        self.int_db = 'ipv_db'
        self.table  = table
        self.header = 'CREATE TABLE IF NOT EXISTS'
        self.body   = body

    def get_int_schema(self):
        schema = ('%s `%s`.`%s` '
                  '%s'
                  'PARTITIONED BY (year STRING, proc_date STRING) '
                  'STORED AS PARQUET ') % (self.header, self.int_db, self.table, self.body)
        return schema

    def get_ext_schema(self):
        schema = ('%s `%s`.`%s` '
                  '%s'
                  'ROW FORMAT DELIMITED FIELDS TERMINATED BY \'\\t\'') % (self.header, self.ext_db, self.table, self.body)
        return schema



