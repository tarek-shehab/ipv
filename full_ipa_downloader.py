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

def get_tasks():
    tasks = {'192.168.250.11' :['2011','2006'],
             '192.168.250.12' :['2012','2007'],
             '192.168.250.13' :['2013','2008'],
             '192.168.250.15' :['2014','2009'],
             '192.168.250.16' :['2015'],
             '192.168.250.17' :['2016',],
             '192.168.250.19' :['2017','2010'],
             '192.168.250.20' :['2018',],
            }
    return tasks.get(socket.gethostbyname(socket.gethostname()))

def get_links(year):
    logging.info('Creating links list')
    try:
        res = []
        url =('https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/%s/') % (year)

        USERAGENT = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        HEADERS = {'User-Agent': USERAGENT}

        page = requests.get(url, headers=HEADERS)

        durl_prefix = 'https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/20'
        content = html.fromstring(page.content)

        file_list =  content.xpath('.//tr/td/a/text()')

        for fl in file_list:
            if fl.endswith('.zip'):
                res.append(durl_prefix + fl[3:5] + '/' + fl)

        return res if len(res) > 0 else False
    except Exception as err:
        logging.error('Failed to create links list')
        logging.error(err)
        return False


#############################################################################
if __name__ == "__main__":

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s', level=logging.INFO)

    target_dir = './source/ipa/'
    tasks = get_tasks()
    if tasks:
        for task in tasks:
            links = get_links(task)
            if links:
                for link in links:
                    logging.info(('Domnloading: %s') % (link))
                    zpf = loader.f_get(link, target_dir)
                    loader.f_unzip(zpf, target_dir)
            else:
                logging.error('No links were extracted!')
    else:
        logging.error('No tasks were extracted!')

    logging.info('Script finished!')


