######################################################################
#
######################################################################
from impala.dbapi import connect 

def init_dbs():
    create_command = 'CREATE DATABASE IF NOT EXISTS '
    dbs = ['ipv_db', 'ipv_ext']
    try:
        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()
        for db in dbs: impala_cur.execute(create_command + db)
        impala_cur.close()
        impala_con.close() 
        print "DB successfully initialized!"
    except Exception as err:
        print "DB Init failed!",err