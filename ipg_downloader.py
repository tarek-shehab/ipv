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

def f_get(url, target):
    name = url[url.rfind('/') + 1:]
    logging.info(('Start downloading <%s>') % (name))
    stime = time.time()
    try:
        if not os.path.exists(target):
            os.makedirs(target)
#        urllib.urlretrieve (url, target + name)
        dfile = wget.download(url, target+name, bar=False)
        with open(dfile, 'rb') as fl:
            part = fl.read(500)

        res = BeautifulSoup(part, "html.parser").find('title')
        ind = bool(res)

        if ind:
            os.remove(dfile)
            raise Exception(res.text)

        if os.path.exists(target + name) and (target+name) != dfile:
            os.remove(target + name)
            os.rename(dfile, target + name)

        logging.info(('File <%s> has downloaded in %s sec.') % (name, str(round(time.time()-stime,2))))
        return target + name
    except Exception as err:
        logging.error('Failed to download file')
        logging.error(err)
        return False

def f_unzip(zfile, target):
    if not zfile: return False
    stime = time.time()
    logging.info(('Start unzipping <%s>') % (zfile))
    try:
        zip_ref = zipfile.ZipFile(zfile, 'r')
        zip_ref.extractall(target)
        zip_ref.close()
        os.remove(zfile)
        logging.info(('File <%s> has unzipped in %s sec.') % (zfile, str(round(time.time()-stime,2))))
    except Exception as err:
        logging.error('Failed to unzip file')
        logging.error(err)
        return False

def get_tasks():
    tasks = {'192.168.250.11' :['2011','2006'],
             '192.168.250.12' :['2012','2007'],
             '192.168.250.13' :['2013','2008'],
             '192.168.250.15' :['2014','2009'],
             '192.168.250.16' :['2015','2010'],
             '192.168.250.17' :['2016',],
             '192.168.250.19' :['2017',],
             '192.168.250.20' :['2018',],
            }
    return tasks[socket.gethostbyname(socket.gethostname())]

def get_links(year):
    logging.info('Creating links list')
    try:
        res = []
        url =('https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/%s/') % (year)

        USERAGENT = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        HEADERS = {'User-Agent': USERAGENT}

        page = requests.get(url, headers=HEADERS)

        durl_prefix = 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/20'
        content = html.fromstring(page.content)

        file_list =  content.xpath('.//tr/td/a/text()')

        for fl in file_list:
            res.append(durl_prefix + fl[3:5] + '/' + fl)
        return res
    except Exception as err:
        logging.error('Failed to create links list')
        logging.error(err)
        return False


#############################################################################
if __name__ == "__main__":

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s', level=logging.INFO)

    target_dir = './source/ipg/'
    tasks = get_tasks()
    for task in tasks:
        links = get_links(task)
        for link in links:
            zpf = f_get(link, target_dir)
            f_unzip(zpf, target_dir)
