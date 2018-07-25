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

hdfs_base_dir = '/ipv'

def init_parsers(f_type):
    modules = {}
#    parsers = {'ipg': ['classification'],
#               'ad': ['assignments']}

    parsers ={'ipg': [
#                       'main',
#                      'applicant',
#                      'inventor',
#                      'assignee',
#                      'd_inventor',
#                      'claims',
                      'classification',
                     ],
              'ad': ['assignments', 'a_assignee', 'a_assignor']}
    for mod in parsers[f_type]: modules[mod] = importlib.import_module('.' + mod, 'parsers')
    return modules

def set_env():
    # libhdfs.so path
    cmd = ["locate", "-l", "1", "libhdfs.so"]
    libhdfsso_path = subprocess\
        .Popen(cmd, stdout=PIPE)\
        .stdout\
        .read()\
        .rstrip()
    os.environ["ARROW_LIBHDFS_DIR"] = os.path.dirname(libhdfsso_path)

    # JAVA_HOME path
    os.environ["JAVA_HOME"] = '/usr/lib/jvm/java-7-oracle-cloudera'

    # classpath
    cmd = ["/usr/bin/hadoop", "classpath", "--glob"]
    hadoop_cp = subprocess\
        .Popen(cmd, stdout=PIPE)\
        .stdout\
        .read()\
        .rstrip()
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
    result = {}
    dig = re.search('\d', file_name)
    result['f_type'] =  file_name[:dig.start()] if dig else False

    if result['f_type'] not in ['ipg', 'ad']:
        raise Exception(('Incorrect file type for XML parser <%s>') % (file_name))

    if result['f_type'] == 'ipg':
        result['proc_date'] =  '20' + re.search("([0-9]{6})", file_name).group(0)
    else: result['proc_date'] =  re.search("([0-9]{8})", file_name).group(0)
    if len(result['proc_date']) != 8:
        raise Exception(('Incorrect date extracted from <%s>') % (file_name))

    return result

def hdfs_connect():
    set_env()
    return pa.hdfs.connect("192.168.250.15", 8020, user='hdfs', driver='libhdfs')

def parse(file_name):
    if not file_name:
        logging.error(('Incorrect argument for XML parser') % (file_name))
        return False
    short_name = os.path.basename(file_name)
    f_prop = parse_file_name(short_name)

    try:
#    if True:
        modules = init_parsers(f_prop['f_type'])
        logging.info(('Start processing %s file') % (short_name))
        start = time.time()
        fstart = start
        hdfs = hdfs_connect()
        xml = splitter.extract_xml_parts(file_name)
        logging.info(('XML file %s has splitted in %s sec.') % (short_name, str(round(time.time()-start, 2))))
        for mod in modules:
            start = time.time()
#            results = []
#            for x in xml:
#                print x
#                results.append(modules[mod].create_line(x))
            pool = Pool(processes = cpu_count()-1 if cpu_count() > 1 else 1)
            results = pool.map(modules[mod].create_line, xml)
            pool.close()
            results = [res for res in results if res]
            logging.info(('Parser <%s> has done in %s sec.') % (mod, str(round(time.time()-start, 2))))

            proc_date =  f_prop['proc_date']

            write_hdfs(hdfs, proc_date, mod, results)
        hdfs.close()
        set_impala_permissions(hdfs_base_dir)
        logging.info(('XML file %s has fully processed in %s sec.') % (short_name, str(round(time.time()-fstart, 2))))
        return f_prop
    except Exception as err:
        logging.error(('XML file %s processing failed!') % (short_name))
        logging.error(err)
        return False

def file_to_hdfs(file_name):
    if not file_name:
        logging.error(('Incorrect file name') % (file_name))
        return False
    short_name = os.path.basename(file_name)

    try:
        start = time.time()
        fstart = start
        with open(file_name, 'rb') as in_file:
#            content = in_file.read()
            lines = in_file.readlines()
        content = []
        for line in lines:
            content.append(line.replace('","','\t').replace('"',''))

        hdfs = hdfs_connect()

        hdfs_name = hdfs_base_dir + '/results/attorney/' + short_name
        print hdfs_name
        print len(content)

        of = hdfs.open(hdfs_name, "wb")
#        of.write(content)
        of.write("".join(content))
        of.close()

        logging.info(('File <%s> hase successfully copied to HDFS') % (short_name))

        hdfs.close()
        set_impala_permissions(hdfs_base_dir)
        return hdfs_name
    except Exception as err:
        logging.error(('Failed to copy <%s> to HDFS!') % (short_name))
        logging.error(err)
        return False

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

def write_hdfs(hdfs, proc_date, modul, results):
    try:
        file_name = hdfs_base_dir + '/results/' + modul + '/data' + proc_date +'.tsv'
        of = hdfs.open(file_name, "wb")
        of.write("".join(results))
        of.close()

        logging.info(('Data for <%s> parser has successfully written') % (modul))
    except Exception as err:
        logging.error(('Failed when writing results for <%s> parser') % (modul))
        logging.error(err)

