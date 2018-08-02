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
    name = url.split('/')[-1]
    logging.info(('Start downloading <%s>') % (name))
    stime = time.time()
    try:
        if not os.path.exists(target):
            os.makedirs(target)
        meta = urllib.urlopen(url).info()
        size = int(meta.getheaders("Content-Length")[0])

        if size < 1000:
            raise Exception(('Seems like file <%s> is empty') % (name))

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
