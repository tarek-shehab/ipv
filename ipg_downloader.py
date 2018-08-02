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
    if value not in ['ipg', 'ipa', 'ad']:
        raise argparse.ArgumentTypeError("%s is an incorrect file type" % value)
    return value

def check_mode(value):
    if value not in ['load', 'parse']:
        raise argparse.ArgumentTypeError("%s is an incorrect procesing mode" % value)
    return value

def check_full(value):
    if value.lower() not in ['y','n','yes','no']:
        raise argparse.ArgumentTypeError("%s is an incorrect loading mode" % value)
    return value

def load_proc(year, ftype, all_files=None):
    target_dir = ('./source/%s/') % (ftype)
    links = lnk.get_links(year, ftype)
    for link in links:
        logging.info(('Downloading: %s') % (link))
        zpf = loader.f_get(link, target_dir)
        loader.f_unzip(zpf, target_dir)

def parse_proc(year, ftype, all_files=None):

#############################################################################
if __name__ == "__main__":

    try:
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument("--type", type=check_type,
                default = 'load',
                help="Type of processed file (ipa, ipg, ad etc.)")
        arg_parser.add_argument("--mode", type=check_mode,
                default = 'ipg',
                help="Processing mode: load or parse")
        arg_parser.add_argument("--full", type=check_full,
                default = 'no',
                help="Loading mode: full load (=yes) or new files only (=no)")
        args = arg_parser.parse_args()


        logging.basicConfig(filename=('./log/%s_%s.log') % (args.type, args.mode),
                            format='[%(asctime)s] %(levelname)s: %(message)s',
                            level=logging.INFO)
        target_year = datetime.now().year

        args = (year, args.type, None if args.full == 'no' else True)

        proc = {'load' : load_proc,
                'parse': parse_proc}

        proc[args.mode](*args)

    except Exception as error:
        logging.error('Failed to process!')
        logging.error(error)
        quit(1)

