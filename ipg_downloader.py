#!/usr/bin/env python
#############################################################################
#
#############################################################################
import sys
import time
import logging
import requests
import ssl
from lxml import html
import zipfile
import subprocess
from subprocess import PIPE
import urllib
import os
import wget
from bs4 import BeautifulSoup
import socket
import routines.ldr_handlers as loader
import routines.lnk_handlers as lnk
from datetime import datetime

#############################################################################
if __name__ == "__main__":

    logging.basicConfig(filename='./log/ipg_downloader.log',
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        level=logging.INFO)

    target_dir = './source/ipg/'
    links = lnk.get_links(datetime.now().year,'ipg')
    if links and len(links) > 0:
        for link in links:
            logging.info(('Downloading: %s') % (link))
            zpf = loader.f_get(link, target_dir)
            loader.f_unzip(zpf, target_dir)
    else: logging.error('No links were extracted for this date!')

    logging.info('Processing finished!')
    logging.info('--------------------')


