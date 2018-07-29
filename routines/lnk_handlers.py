#############################################################################
#
#############################################################################
import user_agents as ua
import logging
import requests
from lxml import html
from impala.dbapi import connect

links = {'ipg': 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/',
         'ipa': 'https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/'
        }

def get_links_list(year, ftype):
    logging.info('Creating links list')
    try:
        if ftype not in ['ipg', 'ipa']: raise Exception('Incorrect file type!')

        res = []

        url =('%s%s/') % (links[ftype], year)

        HEADERS = {'User-Agent': ua.get_user_agent()}

        page = requests.get(url, headers=HEADERS)

        durl_prefix = ('%s20') % (links[ftype])

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

def get_links(year, ftype):
    try:
        if ftype not in ['ipg', 'ipa']: raise Exception('Incorrect file type!')
        links = get_links_list(year, ftype)
        if not links: raise Exception()
        tbl_preffix = {'ipg':'grant','ipa':'application'}[ftype]

        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()
        query = ('SELECT DISTINCT proc_date FROM `ipv_db`.`%s_main` WHERE SUBSTR(proc_date,1,4) = \'%s\'') % (tbl_preffix, year)
        impala_cur.execute(query)
        dates = [elm[0][2:] for elm in impala_cur.fetchall()]
        impala_cur.close()
        impala_con.close()
        res = []
        for elm in links:
            if (elm[-4:].lower() != '.zip') or (elm[-10:-4] in dates): continue
            res.append(elm)

        return res
    except Exception as err:
        logging.error('Failed to get links to download')
        logging.error(err)
        return False

