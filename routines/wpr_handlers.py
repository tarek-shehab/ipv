######################################################################
#
######################################################################
from multiprocessing import Pool,cpu_count
import xml_splitter as splitter
import importlib
import logging
import os
import re
import time
import pyarrow as pa
import subprocess
from subprocess import PIPE
import hashlib
from datetime import datetime

cfg = importlib.import_module('.cfg', 'config')

def init_parsers(f_type):
    modules = {}

    parsers = cfg.active_parsers

    for mod in parsers[f_type]: modules[mod] = importlib.import_module('.' + mod, 'parsers.'+f_type)
    return modules

def set_env():
    # libhdfs.so path
    cmd = ["locate", "-l", "1", "libhdfs.so"]
    libhdfsso_path = subprocess.check_output(cmd).strip()
    os.environ["ARROW_LIBHDFS_DIR"] = os.path.dirname(libhdfsso_path)

    # JAVA_HOME path
    os.environ["JAVA_HOME"] = '/usr/lib/jvm/java-7-oracle-cloudera'

    # classpath
    cmd = ["/usr/bin/hadoop", "classpath", "--glob"]
    hadoop_cp = subprocess.check_output(cmd).strip()

    if "CLASSPATH" in os.environ:
        os.environ["CLASSPATH"] = os.environ["CLASSPATH"] + ":" + hadoop_cp
    else:
        os.environ["CLASSPATH"] = hadoop_cp

def set_impala_permissions(base_dir):
    try:
        logging.info('Changing HDFS files permissions')
        cmd = ["sudo", "-u", "hdfs", "hdfs", "dfs", "-chown", "-R", "impala:supergroup", base_dir]
        subprocess.Popen(cmd, stdout=PIPE)
        cmd = ["sudo", "-u", "hdfs", "hdfs", "dfs", "-chmod", "-R", "777", base_dir]
        subprocess.Popen(cmd, stdout=PIPE)
    except Exception as err:
        logging.error('Can\'t change HDFS files permissions!')
        logging.error(err)

def parse_file_name(file_name):
    name_templates = {'att'  : {'template':'WebRoster.txt', 'prefix': '' , 'reg': None},
                      'fee_m': {'template':'MaintFeeEvents_','prefix': '', 'reg': '([0-9]{8})'},
                      'fee_d': {'template':'MaintFeeEventsDesc_','prefix': '', 'reg': '([0-9]{8})'},
                      'ad'   : {'template':'ad', 'prefix': '', 'reg': '([0-9]{8})'},
                      'ipa'  : {'template':'ipa','prefix': '20', 'reg': '([0-9]{6})'},
                      'ipg'  : {'template':'ipg','prefix': '20', 'reg': '([0-9]{6})'},
                      'pg'   : {'template':'pg','prefix': '20', 'reg': '([0-9]{6})'},
                      'pa'   : {'template':'pa','prefix': '20', 'reg': '([0-9]{6})'},
                     }
    result = {}

    result['f_type'] = False
    for ftype in name_templates:
        if file_name.startswith(name_templates[ftype]['template']):
            result['f_type'] = ftype
            break

    if result['f_type'] not in list(cfg.active_parsers.keys()):
        raise Exception(('Incorrect file type for File parser <%s>') % (file_name))

    if name_templates[result['f_type']]['reg']:
        result['proc_date'] = name_templates[result['f_type']]['prefix'] + \
                              re.search(name_templates[result['f_type']]['reg'], file_name).group(0)
    else: result['proc_date'] = str(datetime.now())[:10].replace('-','')


    if len(result['proc_date']) != 8:
        raise Exception(('Incorrect date extracted from <%s>') % (file_name))

    return result

def hdfs_connect():
#    set_env()
    return pa.hdfs.connect("192.168.250.15", 8020, user='hdfs', driver='libhdfs')

def parse_xml(*args):
    file_name = args[0]
    f_prop = args[1]
    modules = args[2]
    short_name = os.path.basename(file_name)
    start = time.time()
    fstart = start
    hdfs = hdfs_connect()
    xml = splitter.extract_xml_parts(file_name)
    logging.info(('XML file %s has splitted in %s sec.') % (short_name, str(round(time.time()-start, 2))))
    for mod in modules:
        start = time.time()
        results = []
#        for part in xml:
#            print "##############################################################"
#            part = part.replace('&',' ')
#            try:
#            results.append(modules[mod].create_line(part))
#            except Exception as err:
#                print part
#                print err
#        try:
        pool = Pool(processes = 5, maxtasksperchild=1000)
#        pool = Pool(processes = 1, maxtasksperchild=1000)
        results = pool.map(modules[mod].create_line, xml)
#
        pool.close()
        pool.join()
#        except Exception as err:
#            print err

        results = [res for res in results if res]
        logging.info(('Parser <%s> has done in %s sec.') % (mod, str(round(time.time()-start, 2))))

        proc_date =  f_prop['proc_date']

        write_hdfs(hdfs, proc_date, f_prop['f_type'], mod, results)
    hdfs.close()
    set_impala_permissions(cfg.hdfs_base_dir)
    logging.info(('XML file %s has fully processed in %s sec.') % (short_name, str(round(time.time()-fstart, 2))))

def parse_txt(*args):
    hdfs_path = ('%s/results/%s/%s/data%s.tsv') % (cfg.hdfs_base_dir, args[1]['f_type'],'fee_main', args[1]['proc_date'])
#    print args[0], hdfs_path
    local_to_hdfs(args[0], hdfs_path)

def parse(file_name):
    if not file_name:
        logging.error(('Incorrect argument for File parser') % (file_name))
        return False

#    try:
    if True:
        short_name = os.path.basename(file_name)
        f_prop = parse_file_name(short_name)

        modules = init_parsers(f_prop['f_type'])
        logging.info(('Start processing %s file') % (short_name))

        workers = {'att'  : [parse_att, (file_name, f_prop)],
                   'fee_m': [parse_txt, (file_name, f_prop)],
                   'fee_d': [parse_fee_d, (file_name, f_prop)],
                   'ad'   : [parse_xml, (file_name, f_prop, modules)],
                   'ipa'  : [parse_xml, (file_name, f_prop, modules)],
                   'ipg'  : [parse_xml, (file_name, f_prop, modules)],
                   'pg'   : [parse_xml, (file_name, f_prop, modules)],
                   'pa'   : [parse_xml, (file_name, f_prop, modules)]
                  }

#        print f_prop
#        print file_name

        workers[f_prop['f_type']][0](*workers[f_prop['f_type']][1])

        return f_prop
#    except Exception as err:
#        logging.error(('XML file %s processing failed!') % (short_name))
#        logging.error(err)
#        return False

def parse_att(*args):
    file_name = args[0]
    f_prop = args[1]
    if not file_name:
        logging.error(('Incorrect file name') % (file_name))
        return False
    short_name = os.path.basename(file_name)
    updated = str(datetime.now())[:10].replace('-','')
    try:
        start = time.time()
        fstart = start
        with open(file_name, 'rb') as in_file:
            lines = in_file.readlines()
        content = []

        for line in lines:
            elm_list = line.strip().split('","')
            if len(elm_list) > 0:
                elm_list[0] = elm_list[0][1:]
                elm_list[-1] = elm_list[-1][:-1]+'\n'
                pk = hashlib.md5(''.join(elm_list[0:4])).hexdigest()
                elm_list.insert(0,updated)
                elm_list.insert(0,pk)
                content.append('\t'.join(elm_list))

        hdfs = hdfs_connect()

        hdfs_name = ('%s/results/att/attorney/data%s.tsv') % (cfg.hdfs_base_dir, f_prop['proc_date'])

        of = hdfs.open(hdfs_name, "wb")
        of.write("".join(content))
        of.close()

        logging.info(('File <%s> hase successfully copied to HDFS') % (short_name))

        hdfs.close()
        set_impala_permissions(cfg.hdfs_base_dir)
        return hdfs_name
    except Exception as err:
        logging.error(('Failed to copy <%s> to HDFS!') % (short_name))
        logging.error(err)
        return False

def parse_fee_d(*args):
    file_name = args[0]
    f_prop = args[1]
    if not file_name:
        logging.error(('Incorrect file name') % (file_name))
        return False
    short_name = os.path.basename(file_name)
    updated = f_prop['proc_date']
    try:
        start = time.time()
        fstart = start
        with open(file_name, 'rb') as in_file:
            lines = in_file.readlines()
        content = []

        for line in lines:
            elm = line.strip()
            if len(elm) > 0:
                content.append(elm[:5].strip()+'\t'+elm[6:].strip()+'\n')

        content.append('FDFDF\t'+updated+'\n')
        hdfs = hdfs_connect()

        hdfs_name = ('%s/results/fee_d/fee_descr/data%s.tsv') % (cfg.hdfs_base_dir, f_prop['proc_date'])

        of = hdfs.open(hdfs_name, "wb")
        of.write("".join(content))
        of.close()

        logging.info(('File <%s> hase successfully copied to HDFS') % (short_name))

        hdfs.close()
        set_impala_permissions(cfg.hdfs_base_dir)
        return hdfs_name
    except Exception as err:
        logging.error(('Failed to copy <%s> to HDFS!') % (short_name))
        logging.error(err)
        return False

def local_to_hdfs(local_file, hdfs_path):
    hdfs_dir = '/'.join(hdfs_path.split('/')[:-1])
    cmds = [["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
            ["hdfs", "dfs", "-copyFromLocal", "-f", local_file, hdfs_path]]

    for cmd in cmds:
        result = subprocess.check_output(cmd).strip()
        if len(result) > 0: raise Exception(result)

    set_impala_permissions(cfg.hdfs_base_dir)
    return True

def write_result(proc_date, modul, results):
    try:
        file_name = os.getcwd() + '/results/' + modul + '/data' + proc_date +'.tsv'
        of = open(file_name, "w")
        of.write("".join(results))
        of.close()
        logging.info(('Data for <%s> parser has successfully written') % (modul))
    except Exception as err:
        logging.error(('Failed when writing results for <%s> parser') % (modul))
        logging.error(err)

def write_hdfs(hdfs, proc_date, ftype, modul, results):
    try:
        file_name = ('%s/results/%s/%s/data%s.tsv') % (cfg.hdfs_base_dir, ftype, modul, proc_date)
        of = hdfs.open(file_name, "wb")
        of.write("".join(results))
        of.close()
        logging.info(('Data for <%s> parser has successfully written') % (modul))
        return file_name
    except Exception as err:
        logging.error(('Failed when writing results for <%s> parser') % (modul))
        logging.error(err)
        return False

