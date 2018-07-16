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
parsers = ['main', 'applicant', 'inventor', 'assignee', 'd-inventor']
modules = {}

for mod in parsers: modules[mod] = importlib.import_module('.' + mod, 'parsers')

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
        logging.error('Changing HDFS files permissions')
        cmd = ["sudo", "-u", "hdfs", "hdfs", "dfs", "-chown", "-R", "impala:supergroup", base_dir]
        subprocess.Popen(cmd, stdout=PIPE)
        cmd = ["sudo", "-u", "hdfs", "hdfs", "dfs", "-chmod", "-R", "777", base_dir]
        subprocess.Popen(cmd, stdout=PIPE)
    except Exception as err:
        logging.error('Can\'t change HDFS files permissions!')
        logging.error(err)


def hdfs_connect():
    set_env()
    return pa.hdfs.connect("192.168.250.15", 8020, user='hdfs', driver='libhdfs')

def parse(file_name):
    try:
        short_name = os.path.basename(file_name)
        logging.info(('Start processing %s file') % (short_name))
        start = time.time()
        fstart = start
        hdfs = hdfs_connect()
        xml = splitter.extract_xml_parts(file_name)
        logging.info(('XML file %s has splitted in %s sec.') % (short_name, str(round(time.time()-start, 2))))
        for mod in modules:
            start = time.time()
            pool = Pool(processes = cpu_count()-1 if cpu_count() > 1 else 1)
            results = pool.map(modules[mod].create_line, xml)
            pool.close()
            results = [res for res in results if res]
            logging.info(('Parser <%s> has done in %s sec.') % (mod, str(round(time.time()-start, 2))))
            proc_date =  re.search("([0-9]{6})", short_name).group(0)
            write_hdfs(hdfs, proc_date, mod, results)
        hdfs.close()
        set_impala_permissions(hdfs_base_dir)
        logging.info(('XML file %s has fully processed in %s sec.') % (short_name, str(round(time.time()-fstart, 2))))
    except Exception as err:
        logging.error(('XML file %s processing failed!') % (short_name))
        logging.error(err)


def write_result(proc_date, modul, results):
    try:
        file_name = os.getcwd() + '/results/' + modul + '/data' + proc_date +'.tsv'
        of = open(file_name, "w")
        of.write("".join(results))
        of.close()
        logging.info(('Data for <%s> parser has successfully wrote') % (modul))
    except Exception as err:
        logging.error(('Failed when writing results for <%s> parser') % (modul))
        logging.error(err)

def write_hdfs(hdfs, proc_date, modul, results):
    try:
        file_name = hdfs_base_dir + '/results/' + modul + '/data' + proc_date +'.tsv'
        of = hdfs.open(file_name, "wb")
        of.write("".join(results))
        of.close()

        logging.info(('Data for <%s> parser has successfully wrote') % (modul))
    except Exception as err:
        logging.error(('Failed when writing results for <%s> parser') % (modul))
        logging.error(err)

