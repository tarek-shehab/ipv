######################################################################
#
######################################################################
from impala.dbapi import connect
import importlib
import logging
import os

models = importlib.import_module('.cfg', 'config').active_models

def init_models(mtype):
    tlist = []
    modules = {}
    tlist = models[mtype]
    if len(tlist) > 0:
        for mod in tlist: modules[mod] = importlib.import_module('.' + mod, 'models.' + mtype)
        return modules
    else: raise Exception('Tables type is incorrect!')

def init_tables(ttype):
#    if True:
    try:
        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()
        modules = init_models(ttype)
        for mod in modules:
            impala_cur.execute(modules[mod].model.get_int_schema(ttype))
            impala_cur.execute(modules[mod].model.get_ext_schema(ttype))
            logging.info(('Table %s has successfully initialized!') % (mod))
        impala_cur.close()
        impala_con.close() 
        return True
    except Exception as err:
        logging.error('Tables initialization failed!')
        logging.error(err)
        return False

def load_tables(properties):
    if (not properties) or properties['f_type'] not in list(models.keys()):
       logging.error('Incorrect argument for tables loader!')
       return False

    modules = init_models(properties['f_type'])

#    if True:
    try:
        if not init_tables(properties['f_type']): raise Exception()
        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()

        for mod in models[properties['f_type']]:
            target_path = ('/ipv/results/%s/%s/data%s.tsv') % (properties['f_type'], mod, properties['proc_date'])

            if properties['f_type'] in ['att', 'ad']:
                if properties['f_type'] == 'att':
                    target_path = '/ipv/results/' + mod + '/WebRoster.txt'

                insert_sql = ('UPSERT INTO TABLE `%s`.`%s` '
                              'SELECT * FROM `%s`.`%s`') % ('ipv_db', modules[mod].model.get_table_name(),
                                                            'ipv_ext', modules[mod].model.get_table_name())
            else:
                insert_sql = ('INSERT OVERWRITE TABLE `%s`.`%s` PARTITION (proc_date=\'%s\') '
                              'SELECT * FROM `%s`.`%s`') % ('ipv_db', modules[mod].model.get_table_name(), properties['proc_date'],
                                                            'ipv_ext',
                                                            modules[mod].model.get_table_name())

            load_sql = ('LOAD DATA INPATH \'%s\' OVERWRITE INTO TABLE `%s`.`%s`') % (target_path, 
                                                                                     'ipv_ext',
                                                                                     modules[mod].model.get_table_name())

#            refresh = ('INVALIDATE METADATA `%s`.`%s`') % ('ipv_ext', modules[mod].model.get_table_name())
#            impala_cur.execute(refresh)

            impala_cur.execute(load_sql)
            logging.info(('Data has successfully loaded into temporary table: %s!') % (modules[mod].model.get_table_name()))
            impala_cur.execute(insert_sql)
            logging.info(('Data has successfully loaded into HDFS table: %s!') % (modules[mod].model.get_table_name()))
        impala_cur.close()
        impala_con.close()
        return True
    except Exception as err:
        logging.error('Tables loading failed!')
        logging.error(err)
        return False