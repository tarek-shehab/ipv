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

parsers = ['main',]
modules = {}

for mod in parsers: modules[mod] = importlib.import_module('.' + mod, 'parsers')

def parse(file_name):
    try:
        short_name = os.path.basename(file_name)
        logging.info(('Start processing %s file') % (short_name))
        start = time.time()
        fstart = start
        xml = splitter.extract_xml_parts(file_name)
        logging.info(('XML file %s has splitted in %s sec.') % (short_name, str(round(time.time()-start, 2))))
        for mod in modules:
            start = time.time()
            pool = Pool(processes = cpu_count()-1 if cpu_count() > 1 else 1)
            results = pool.map(modules[mod].create_line, xml)
            pool.close()
            logging.info(('Parser <%s> has done in %s sec.') % (mod, str(round(time.time()-start, 2))))
            proc_date =  re.search("([0-9]{6})", short_name).group(0)
            write_result(proc_date, mod, results)
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

