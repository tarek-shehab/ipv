######################################################################
#
######################################################################
from impala.dbapi import connect
import importlib
import logging
import os

#models = ['main', 'applicant', 'inventor', 'assignee', 'd_inventor', 'claims']

models = ['classification',]

modules = {}

for mod in models: modules[mod] = importlib.import_module('.' + mod, 'models')

def init_tables():
    try:
        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()
        for mod in modules:
            impala_cur.execute(modules[mod].model.get_int_schema())
            impala_cur.execute(modules[mod].model.get_ext_schema())
            logging.info(('Table %s has successfully initialized!') % (mod))
        impala_cur.close()
        impala_con.close() 
    except Exception as err:
        logging.error('Tables initialization failed!')
        logging.error(err)

def load_tables(proc_date):
    try:
        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()

        for mod in modules:
#            target_path = os.getcwd() + '/results/' + mod + '/data' + proc_date + '.tsv'
            target_path = '/ipv/results/' + mod + '/data' + proc_date + '.tsv'
            load_sql = ('LOAD DATA INPATH \'%s\' OVERWRITE INTO TABLE `%s`.`%s`') % (target_path, 'ipv_ext', mod)
            insert_sql = ('INSERT OVERWRITE TABLE `%s`.`%s` PARTITION (proc_date=\'%s\') '
                          'SELECT * FROM `%s`.`%s`') % ('ipv_db', mod, proc_date, 'ipv_ext', mod)
            refresh = ('INVALIDATE METADATA `%s`.`%s`') % ('ipv_ext', mod)
            impala_cur.execute(refresh)
            impala_cur.execute(load_sql)
            logging.info(('Data has successfully loaded into temporary table: %s!') % (mod))
            impala_cur.execute(insert_sql)
            logging.info(('Data has successfully loaded into HDFS table: %s!') % (mod))

        impala_cur.close()
        impala_con.close()
    except Exception as err:
        logging.error('Tables loading failed!')
        logging.error(err)
