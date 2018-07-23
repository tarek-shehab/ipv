######################################################################
#
######################################################################
from impala.dbapi import connect
import importlib
import logging
import os

models = {'ipg': ['classification'],
          'ad': ['assignments', 'a_assignee'],
          'att': ['attorney']}

#models = {'ipg': ['main', 'applicant', 'inventor', 'assignee', 'd_inventor', 'claims'],
#           'ipa': ['application']}

def init_models(mtype='all'):
    modules = {}
    tlist = []
    if mtype == 'all':
        for tp in models: tlist.extend(models[t])
    else: tlist = models[mtype]
    if len(tlist) > 0:
        for mod in tlist: modules[mod] = importlib.import_module('.' + mod, 'models')
        return modules
    else: raise Exception('Tables type is incorrect!')

def init_tables(ttype='all'):
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
    if not properties and properties['f_type'] not in ['ipg','ad']:
       logging.error(('Incorrect argument for tables loader!') % (name))
       return False
    if True:
#    try:
        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()

        for mod in models[properties['f_type']]:
            if properties['f_type'] == 'att':
                target_path = '/ipv/results/' + mod + '/WebRoster.txt'
                insert_sql = ('LOAD DATA INPATH \'%s\' OVERWRITE INTO TABLE `%s`.`%s`') % (target_path, 'ipv_db', mod)
            else:
#                target_path = os.getcwd() + '/results/' + mod + '/data' + proc_date + '.tsv'
                target_path = '/ipv/results/' + mod + '/data' + properties['proc_date'] + '.tsv'
                load_sql = ('LOAD DATA INPATH \'%s\' OVERWRITE INTO TABLE `%s`.`%s`') % (target_path, 'ipv_ext', mod)
                insert_sql = ('INSERT OVERWRITE TABLE `%s`.`%s` PARTITION (year=\'%s\', proc_date=\'%s\') '
                              'SELECT * FROM `%s`.`%s`') % ('ipv_db', mod, properties['proc_date'][:4], 
                                                            properties['proc_date'][4:], 'ipv_ext', mod)
                refresh = ('INVALIDATE METADATA `%s`.`%s`') % ('ipv_ext', mod)
                impala_cur.execute(refresh)
                impala_cur.execute(load_sql)
                logging.info(('Data has successfully loaded into temporary table: %s!') % (mod))
            print insert_sql
            impala_cur.execute(insert_sql)
            logging.info(('Data has successfully loaded into HDFS table: %s!') % (mod))
        impala_cur.close()
        impala_con.close()
        return True
#    except Exception as err:
#        logging.error('Tables loading failed!')
#        logging.error(err)
#        return False