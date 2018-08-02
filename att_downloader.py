#!/usr/bin/env python
#############################################################################
#
#############################################################################
import sys
import time
import logging
import zipfile
import subprocess
import os
import routines.tbl_handlers as tbl
import routines.ldr_handlers as loader
import routines.wpr_handlers as copier


#############################################################################
if __name__ == "__main__":



    copier.local_to_hdfs('./source/fee/MaintFeeEvents_20180730.txt', '/ipv/results/temp/fee')
    quit()
    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s', level=logging.INFO)
    stime = time.time()
    target_dir = './source/att/'
    link = 'http://www.uspto.gov/attorney-roster/attorney.zip'
    tbl.init_tables('att')
    zpf = loader.f_get(link, target_dir)
    loader.f_unzip(zpf, target_dir)
    copier.file_to_hdfs(target_dir + 'WebRoster.txt' )
    tbl.load_tables({'f_type':'att'})
    logging.info(('Script finished in %s sec.!') % (str(round(time.time()-stime,2))))


