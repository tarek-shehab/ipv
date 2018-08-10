#!/usr/bin/env python
#############################################################################
#
#############################################################################
import sys
import time
import logging
import os
import pprint
import requests
import random
import json
from os import walk
import routines.dbs_handlers as dbs
import routines.tbl_handlers as tbl
import routines.wpr_handlers as parser
import routines.user_agents as ua
from multiprocessing.dummy import Pool
#from multiprocessing import Pool
from retrying import retry
from lxml import etree
from datetime import datetime
from datetime import timedelta
from impala.dbapi import connect
import importlib

def write_local(arr,name):
    res = ''
    for elm in arr:
        res += u"\t".join(elm).encode('utf-8').strip()+"\n"
    with open('./source/temp/' + name, 'w') as fl:
       fl.write(res)

@retry(wait_exponential_multiplier=10, wait_exponential_max=5000, stop_max_delay=50000)
def get_data(app_id):
    try:
        headers = {'User-Agent': ua.custom_user_agent[random.randint(0,len(ua.custom_user_agent)-1)],
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        api_call = ('{\"searchText\":\"%s\",\"qf\":\"applId\"}') % (app_id)
        url = 'https://ped.uspto.gov/api/queries'
        response = requests.post(url, data=api_call, headers=headers, timeout = 15).text

        try:
            api_content = json.loads(response)
        except Exception as err:
            tree = etree.HTML(response)
            error = ('appID: %s, Error: %s') % (app_id, tree.xpath('.//body/h1')[0].text)
            raise Exception(error)

        stat = api_content['queryResults']['searchResponse']['response']['docs']
        res = []
        if stat:
            keys = ['appType', 'appStatus', 'appStatusDate', 'appAttrDockNumber', 'appCustNumber']
            res = [app_id]
            for k in keys: res.append(stat[0].get(k, '-'))

            thist = stat[0].get('transactions', [])
            pool = []
            if thist:
                keys = ['recordDate', 'code', 'description']
                for he in thist:
                    buf = []
                    for k in keys: buf.append(he.get(k, '-'))
                    pool.append(buf)
            res.append(pool)
        return res
    except Exception as err:
        logging.error(err)
        raise Exception('Request failed'+str(err))

def run_query(query):
    impala_con = connect(host='localhost')
    impala_cur = impala_con.cursor()
    impala_cur.execute(query)
    result = impala_cur.fetchall()
    impala_cur.close()
    impala_con.close()
    return result

def get_partitions():
    start_date = str(datetime.now() - timedelta(days=35))[:10].replace('-','')
    sql = ('SELECT distinct proc_date from '
           '`ipv_db`.`application_main` '
           'WHERE proc_date >= \'%s\'') % (start_date)
    return run_query(sql)

def get_ids(partition):
    sql = ('SELECT distinct app_id FROM '
           '`ipv_db`.`application_main` '
           'WHERE proc_date = \'%s\'') % (partition)
    return [ids[0] for ids in run_query(sql)]

def start_pool(ids):
    start = time.time()
    logging.info(('Numbers of appIds in partition: %s') % (str(len(ids))))
    pool = Pool(processes=10)
    results = pool.map(get_data, ids)
    pool.close()
    pool.join()
    logging.info(('Ids processing completed %s sec') % (str(round(time.time()-start, 2))))
    return [res for res in results if res]

def split_result(result):
    info_pool = []
    trh_pool = []
    for elm in result:
        info_pool.extend([elm[:6]])
        for tr in elm[-1]:
            trh_pool.extend([[elm[0]] + tr])
    info_res = ''
    trh_res = ''

    for elm in info_pool: info_res += u"\t".join(elm).encode('utf-8').strip()+"\n"
    for elm in trh_pool: trh_res += u"\t".join(elm).encode('utf-8').strip()+"\n"
    print info_res
    
    return {'ainf': info_res, 'thist': trh_res}

#############################################################################
if __name__ == "__main__":

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s', level=logging.INFO)
    pp = pprint.PrettyPrinter(indent=1)
    ids = [
        '29458406','29445064','29462581','29472684','29462338','29464705',
        '29450795','29469676','29442338','29428882','29466404','29417409',
        '29404599','29436325','29479926','29475504','29404579','29420621',
        '29475188','29472462','29424216','29449156','29420613','29420619',
        '29420624','29420626','29420627','29420631','29420618','29419594',
        '29472186','29475178','29475187','29475502','29483676','29483684',
        '29483686','29483701','29465804','29483699','29407995','29453941',
        '29451063','29391095','29458303','29458058','29462805','29452942',
        '29440330','29452829'
        ]

    ids = ids
    t = time.time()
    i = 0
    properties = {'proc_date': str(datetime.now())[:10].replace('-','')}

    cfg = importlib.import_module('.cfg', 'config')
    models = cfg.active_models

    models = {key: models[key] for key in ['thist','ainf']}

    for model in models:
        tbl.init_tables(model)


#    for partition in get_partitions():
    for partition in [1]:
        start = time.time()
        logging.info(('Start processing partition: %s') % (partition))
#        final = split_result(start_pool(get_ids(partition)))
        final = split_result(start_pool(ids))
        parser.set_env()
        hdfs_conn = parser.hdfs_connect()
        for model in models:
            for table in models[model]:
                parser.write_hdfs(hdfs_conn,
                                  properties['proc_date'],
                                  model, table, final[model])


            properties['f_type'] = model
            parser.set_impala_permissions(cfg.hdfs_base_dir)
            tbl.load_tables(properties)
        hdfs_conn.close()

        logging.info(('Partition processing completed in %s sec') % (str(round(time.time()-start, 2))))

    logging.info(('Overall processin time: %s sec') % (str(round(time.time()-t, 2))))
