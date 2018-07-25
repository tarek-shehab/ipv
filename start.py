#!/usr/bin/env python
#############################################################################
#
#############################################################################
import xml.dom.minidom
import xml.etree.ElementTree as ET
import sys
import time
from multiprocessing import Pool,cpu_count
from impala.dbapi import connect 

#############################################################################
if __name__ == "__main__":

    import logging
    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s', level=logging.INFO)
    import routines.wpr_handlers as wpr

#    wpr.parse('ad20180101.xml')
    wpr.parse('ipg180102.xml')

#    import routines.dbs_handlers as dbi
    import routines.tbl_handlers as tables

#    dbi.init_dbs()
#    tables.init_tables()

#    tables.load_tables('201801','ad')

#    process(extract_xml_parts("ipg180102.xml"),"./results/main/data.tsv")


