#!/usr/bin/env python
#############################################################################
#
#############################################################################
import sys
import os
from os import walk
import time
import logging
import zipfile
import routines.dbs_handlers as dbs
import routines.ldr_handlers as loader
import routines.tbl_handlers as tbl
import routines.wpr_handlers as parser
import routines.lnk_handlers as lnk
from datetime import datetime
import argparse

def check_type(value):
    if value not in ['ipg', 'ipa', 'ad', 'fee', 'att', 'pg', 'pa']:
        raise argparse.ArgumentTypeError("%s is an incorrect file type" % value)
    return value

def check_mode(value):
    if value not in ['load', 'parse']:
        raise argparse.ArgumentTypeError("%s is an incorrect procesing mode" % value)
    return value

def check_year(value):
    if int(value) < 2001 and int(value) > 2050:
        raise argparse.ArgumentTypeError("%s is an incorrect procesing year" % value)
    return value

def load_proc(year, ftype, all_files=None):
    target_dir = ('./source/%s/') % (ftype)
    links = lnk.get_links(year, ftype, all_files)
    logging.info(('Found %s files to download') % (str(len(links))))
#    quit()
    for link in links:
        stime = time.time()
        logging.info(('Downloading: %s') % (link))
        zpf = loader.f_get(link, target_dir)
        loader.f_unzip(zpf, target_dir)
        logging.info(('File %s has been downloaded and unzipped in %s sec.') % (link.split('/')[-1],
                       str(time.time()-stime).split('.')[0]))

def parse_proc(year, ftype, all_files=None):
    stime = time.time()
    source_dir = ('./source/%s/') % (ftype)
    processed_dir = ('./source/processed/%s/') % (ftype)

    parser.set_env()
    flist = []
    for (dirpath, dirnames, filenames) in walk(source_dir):
        flist.extend(filenames)
        break

    logging.info(('Found %s files to process') % (len(flist)))
    for fl in flist:
        stime = time.time()
#        parser.parse(source_dir + fl)
#        continue
        if tbl.load_tables(parser.parse(source_dir + fl)):
            if not os.path.exists(processed_dir):
                os.makedirs(processed_dir)
#            os.rename(source_dir + fl, processed_dir + fl)
            logging.info(('File %s successfully parsed and loaded into Impala table in %s sec.') % (fl,
                          str(round(time.time()-stime,2))))
        else:
            raise Exception(('Failed to process %s') % (fl))

        logging.info(('Total processing time: %s') % (str(round(time.time()-stime,2))))

#############################################################################
if __name__ == "__main__":

#    try:
    if True:
        descr = ('IPV Patent information files Downloader&Parser v0.1\n'
                 'Author: Alex Kovalsky')
        arg_parser = argparse.ArgumentParser(description=descr)
        arg_parser.add_argument("--type", type=check_type,
                default = '',
                help="Type of processed file (ipa, ipg, ad etc.)")
        arg_parser.add_argument("--mode", type=check_mode,
                default = '',
                help="Processing mode: load or parse")
        arg_parser.add_argument("--year", type=check_year,
                default = datetime.now().year,
                help="Year for source file downloads")
        arg_parser.add_argument("--full", action='store_true',
                help="Loading mode: full load , instead of only new files")
        arg_parser.add_argument("--init_tables", action='store_true',
                help="Force tables initialization before processing")
        arg_parser.add_argument("--init_databases", action='store_true',
                help="Force databases initialization before processing")

        if len(sys.argv)==1:
            arg_parser.print_help()
            sys.exit(1)

        args = arg_parser.parse_args()


#        logging.basicConfig(filename=('./log/%s_%s.log') % (args.type, args.mode),
#                            format='[%(asctime)s] %(levelname)s: %(message)s',
#                            level=logging.INFO)
        logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s',
                            level=logging.INFO)

#        target_year = datetime.now().year
        target_year = '2002'

        arg = (args.year, args.type, args.full)

        proc = {'load' : load_proc,
                'parse': parse_proc}

        if args.init_databases: dbs.init_dbs()
        if args.init_tables: tbl.init_tables(args.type)

        proc[args.mode](*arg)

#    except Exception as error:
#        logging.error('Failed to process!')
#        logging.error(error)
#        quit(1)
