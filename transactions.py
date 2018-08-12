#!/usr/bin/env python
#############################################################################
#
#############################################################################
import sys
import time
import logging
import os
import pprint
import requests
import random
import json
from os import walk
import routines.dbs_handlers as dbs
import routines.tbl_handlers as tbl
import routines.wpr_handlers as parser
import routines.user_agents as ua

#############################################################################
if __name__ == "__main__":

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s', level=logging.INFO)
    pp = pprint.PrettyPrinter(indent=1)

    headers = {'User-Agent': ua.custom_user_agent[random.randint(0,len(ua.custom_user_agent)-1)],
               'Content-Type': 'application/json',
               'Accept': 'application/json'}
    api_call = '{\"searchText\":\"15925062\",\"qf\":\"applId\"}'
    url = 'https://ped.uspto.gov/api/queries'
    api_content = json.loads(requests.post(url, data=api_call, headers=headers, timeout = 5).text)
    pp.pprint(api_content)
