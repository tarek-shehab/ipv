#!/usr/bin/env python
#############################################################################
#
#############################################################################
import sys
import time
import logging
import os
from os import walk
import routines.dbs_handlers as dbs
import routines.tbl_handlers as tbl
import routines.wpr_handlers as parser

#############################################################################
if __name__ == "__main__":

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s',
                        level=logging.INFO)

    source_dir = './source/ipg/'
    processed_dir = './source/processed/ipg/'

    ftime = time.time()
    parser.set_env()
    if dbs.init_dbs():

#        flist = ['ipg170103.xml']
        flist = []
        for (dirpath, dirnames, filenames) in walk(source_dir):
            flist.extend(filenames)
            break

        logging.info(('Found %s files to process') % (len(flist)))
        for fl in flist:
            stime = time.time()
            if tbl.load_tables(parser.parse(source_dir + fl)):
                if not os.path.exists(processed_dir):
                    os.makedirs(processed_dir)
                os.rename(source_dir + fl, processed_dir + fl)
                logging.info(('File %s successfully parsed and loaded into Impala table in %s sec.') % (fl,
                              str(round(time.time()-stime,2))))
            else:
                logging.error(('Failed to process %s') % (fl))
                quit(1)

        logging.info(('Total processing time: %s') % (str(round(time.time()-ftime,2))))
    else:
        logging.error('Failed to initialize Databases')