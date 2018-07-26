######################################################################
#
######################################################################
class tbl_model():

    def __init__(self, table, body):
        self.ext_db = 'ipv_ext'
        self.int_db = 'ipv_db'
        self.table  = table
        self.header = 'CREATE TABLE IF NOT EXISTS'
        self.body_int = body[0]
        self.body_ext = body[1]

    def get_int_schema(self,ftype=False):
        if ftype == 'att':
            schema = ('%s `%s`.`%s` '
                      '%s '
                      'STORED AS KUDU') % (self.header, self.int_db, self.table, self.body_int)
        else:
            schema = ('%s `%s`.`%s` '
                      '%s '
                      'PARTITIONED BY (year STRING, month STRING, day STRING) '
                      'STORED AS PARQUET ') % (self.header, self.int_db, self.table, self.body_int)
        return schema

    def get_ext_schema(self,ftype=False):
        if ftype == 'att':
            schema = ('%s `%s`.`%s` '
                      '%s '
                      'ROW FORMAT DELIMITED FIELDS TERMINATED BY \'\\t\'') % (self.header, self.ext_db, self.table, self.body_ext)
        else:
            schema = ('%s `%s`.`%s` '
                      '%s '
                      'ROW FORMAT DELIMITED FIELDS TERMINATED BY \'\\t\'') % (self.header, self.ext_db, self.table, self.body_int)
        return schema

    def get_table_name(self):
        return self.table



