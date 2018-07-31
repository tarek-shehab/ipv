#!/usr/bin/env python
#############################################################################
#
#############################################################################
import sys
import time
import logging
import zipfile
import routines.ldr_handlers as loader
import routines.lnk_handlers as lnk
from datetime import datetime
import argparse

def check_type(value):
    if value not in ['ipg', 'ipa']:
        raise argparse.ArgumentTypeError("%s is an incorrect file type" % value)
    return value

def check_(value):
    if value not in ['load', 'parse']:
        raise argparse.ArgumentTypeError("%s is an incorrect procesing mode" % value)
    return value

def load_proc(year, ftype):
    target_dir = ('./source/%s/') % (ftype)
    links = lnk.get_links(year, ftype)
    if links and len(links) > 0:
        for link in links:
            logging.info(('Downloading: %s') % (link))
            zpf = loader.f_get(link, target_dir)
            loader.f_unzip(zpf, target_dir)
    else: logging.error('No links were extracted for this date!')

#############################################################################
if __name__ == "__main__":


    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--type", type=check_type,
            default = False,
            help="Type of processed file (ipa, ipg, ad etc.)")
    arg_parser.add_argument("--mode", type=check_mode,
            default = False,
            help="Processing mode: load or parse")
    args = arg_parser.parse_args()


    logging.basicConfig(filename=('./log/%s_%s.log') % (args.type, args.mode),
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        level=logging.INFO)


    logging.info('Processing finished!')
    logging.info('--------------------')


