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
from multiprocessing import Value, Array

def write_local(arr,name):
    res = ''
    for elm in arr:
        res += u"\t".join(elm).encode('utf-8').strip()+"\n"
    with open(name, 'w') as fl:
       fl.write(res)

@retry(wait_exponential_multiplier=10, wait_exponential_max=5000, stop_max_delay=60000)
def get_data(app_id):
    global retries, failed_ids, no_info, no_thist, ni_ind, nh_ind, sind, progress, cur, dec
    if cur.value == dec.value:
        cur.value = 0
        progress.value += 1
        logger.info(('%s Completed\n') % (str(progress.value * 10)))
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
            else:
                no_thist[nh_ind.value] = int(app_id)
                nh_ind.value += 1
            res.append(pool)
        else:
            no_info[ni_ind.value] = int(app_id)
            ni_ind.value += 1

        cur.value += 1
        return res
    except Exception as err:
        retries.value += 1
#        return []
#        logging.error(err)
        raise Exception('Request failed'+str(err))

def get_data_wrapper(app_id):
    try:
        return get_data(str(app_id))
    except Exception:
        failed_ids[fl_ind.value] = int(app_id if app_id else 0)
        fl_ind.value += 1
        return []

def run_query(query):
    impala_con = connect(host='localhost')
    impala_cur = impala_con.cursor()
    impala_cur.execute(query)
    result = impala_cur.fetchall()
    impala_cur.close()
    impala_con.close()
    return result

def get_partitions(year):
    start_date = str(datetime.now() - timedelta(days=35))[:10].replace('-','')
#    start_date = '2017'
    start_date = year
#    sql = ('SELECT distinct proc_date from '
#           '`ipv_db`.`application_main` '
#           'WHERE proc_date >= \'%s\'') % (start_date)
    sql = ('SELECT distinct proc_date AS pd from '
           '`ipv_db`.`application_main` '
           'WHERE SUBSTR(proc_date,1,4) = \'%s\' ORDER BY pd') % (start_date)
    return [ids[0] for ids in run_query(sql)]

def get_ids(partition):
    sql = ('SELECT distinct app_id FROM '
           '`ipv_db`.`application_main` '
           'WHERE proc_date = \'%s\'') % (partition)
    return [ids[0] for ids in run_query(sql)]

def start_pool(ids):
    start = time.time()
    logging.info(('Numbers of appIds in partition: %s') % (str(len(ids))))
    pool = Pool(processes=10)
    results = pool.map(get_data_wrapper, ids)
    pool.close()
    pool.join()
    logging.info(('Ids processing completed %s sec') % (str(round(time.time()-start, 2))))
    return [res for res in results if res]

def split_result(result, partition):
    info_pool = []
    trh_pool = []
    for elm in result:
        info_pool.extend([elm[:6] + [partition]])
        for tr in elm[-1]:
            trh_pool.extend([[elm[0]] + tr + [partition]])
    info_res = ''
    trh_res = ''

    for elm in info_pool: info_res += u"\t".join(elm).encode('utf-8').strip()+"\n"
    for elm in trh_pool: trh_res += u"\t".join(elm).encode('utf-8').strip()+"\n"

    return {
        'ainf': info_res,
        'thist': trh_res,
        'ainf_len': len(info_pool),
        'thist_len': len(trh_pool)
        }

def get_tasks():
    tasks = {'192.168.250.11' :['2006','2007'],
             '192.168.250.12' :['2008','2009'],
             '192.168.250.13' :['2010','2011'],
             '192.168.250.15' :['2012','2013'],
             '192.168.250.16' :['2014','2015'],
             '192.168.250.17' :['2016',],
             '192.168.250.19' :['2017',],
             '192.168.250.20' :['2018',],
            }
    return tasks.get(socket.gethostbyname(socket.gethostname()))

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
    parser.set_env()
    i = 0
    properties = {'proc_date': str(datetime.now())[:10].replace('-','')}

    cfg = importlib.import_module('.cfg', 'config')
    models = cfg.active_models

    models = {key: models[key] for key in ['thist','ainf']}

    for model in models:
        tbl.init_tables(model)


    for partition in get_partitions('2018'):

        failed_ids = Array('i',20000)
        retries = Value('i',0)
        no_info = Array('i',20000)
        no_thist = Array('i',20000)
        fl_ind = Value('i',0)
        ni_ind = Value('i',0)
        nh_ind = Value('i',0)
        progress = Value('i',0)
        dec = Value('i',0)
        cur = Value('i',0)
        start = time.time()
        logging.info(('Start processing partition: %s') % (partition))
        ids = get_ids(partition)
        dec.value = len(ids) // 10
        final = split_result(start_pool(ids), partition)
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
        logging.info(('STAT: Partition                =  %s') % (partition))
        logging.info(('STAT: Total Ids in partition   =  %s') % (str(len(ids))))
        logging.info(('STAT: Total Ids extracted      =  %s') % (str(final['ainf_len'])))
        logging.info(('STAT: Total tr.history records =  %s') % (str(final['thist_len'])))
        logging.info(('STAT: Numbers of retries       =  %s') % (str(retries.value)))
        logging.info(('STAT: Failed Ids numbers       =  %s') % (str(len([1 for e in failed_ids if e != 0]))))
        logging.info(('STAT: Ids with no app.info     =  %s') % (str(len([1 for e in no_info if e != 0]))))
        logging.info(('STAT: Ids with no tr.history   =  %s') % (str(len([1 for e in no_thist if e != 0]))))
        logging.info(('STAT: Average req/sec. ratey   =  %s') % (str(round(len(ids)/(time.time()-start), 2))))

        res_path = './log/thist/'
        marker = str(int(time.time()))
        logarr = {
            'failed_ids': failed_ids,
            'no_info': no_info,
            'no_thist': no_thist
            }
        for elm in logarr:
            name = ('%s%s_%s.txt') % (res_path, elm, partition)
            write_local([[str(e)] for e in logarr[elm] if e != 0], name)


        logging.info(('Partition processing completed in %s sec') % (str(round(time.time()-start, 2))))
#        break
    logging.info(('Overall processin time: %s sec') % (str(round(time.time()-t, 2))))
