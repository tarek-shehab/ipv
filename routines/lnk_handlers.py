#############################################################################
#
#############################################################################
import user_agents as ua
import logging
import requests
import urllib
from lxml import html
from impala.dbapi import connect
from datetime import datetime
import importlib
cfg = importlib.import_module('.cfg', 'config')

def get_possible_ftypes():
   return [t if not t.startswith('fee') else 'fee' for t in cfg.active_parsers.keys()]

def get_links_list(year, ftype):
    logging.info('Creating links list')
    if ftype not in get_possible_ftypes(): raise Exception('Incorrect file type!')
    res = []
    if ftype == 'ad': 
        url = cfg.dwl_links[ftype]
        durl_prefix = url
    elif ftype in ['fee','att']:
        return [cfg.dwl_links[ftype]]
    else:
        url =('%s%s/') % (cfg.dwl_links[ftype], year)
        durl_prefix = ('%s20') % (cfg.dwl_links[ftype])

    HEADERS = {'User-Agent': ua.get_user_agent()}

    page = requests.get(url, headers=HEADERS)

    content = html.fromstring(page.content)

    file_list =  content.xpath('.//tr/td/a/text()')

    for fl in file_list:
        if fl.endswith('.zip') and (fl.split('/')[-1].find('-') < 0):
            if ftype == 'ad':
                res.append(durl_prefix + fl)
            elif ftype in ['pg','pa']: res.append(durl_prefix + fl[2:4] + '/' + fl)
            else:res.append(durl_prefix + fl[3:5] + '/' + fl)

    print res
    return res if len(res) > 0 else False


def get_links(year, ftype, full_list=None):

    if ftype not in get_possible_ftypes(): raise Exception('Incorrect file type!')
    links = get_links_list(year, ftype)
    if not links: raise Exception('No links were extracted for this year')
#    print links
    res = []
    if not full_list:
        tbl_preffix = {'ipg' :'grant',
                       'ipa' :'application',
                        'ad' :'assignment',
                        'att':'attorney',
                        'fee':'fee'}[ftype]

        impala_con = connect(host='localhost')
        impala_cur = impala_con.cursor()
        if ftype in ['ipg', 'ipa', 'pg', 'pa', 'fee']:
            query = ('SELECT DISTINCT proc_date FROM `ipv_db`.`%s_main` WHERE SUBSTR(proc_date,1,4) = \'%s\'') % (tbl_preffix, year)
        elif ftype in ['att']:
            query = ('SELECT updated FROM `ipv_db`.`%s` LIMIT 1') % (tbl_preffix)
        else:
            query = ('SELECT DISTINCT last_update FROM `ipv_db`.`%s_main` WHERE SUBSTR(last_update,1,4) = \'%s\'') % (tbl_preffix, year)

        impala_cur.execute(query)
        dates = [elm[0][2:] for elm in impala_cur.fetchall()]
        impala_cur.close()
        impala_con.close()

        if ftype in ['fee','att']:
            meta = urllib.urlopen(links[0]).info()
            date = datetime.strptime(meta.getheaders('Last-Modified')[0],'%a, %d %b %Y %H:%M:%S %Z')
            date = datetime.strftime(date, '%Y%m%d')
            if date[2:] not in dates: res = links[:]
        else:
            for elm in links:
                if (elm[-4:].lower() != '.zip') or (elm[-10:-4] in dates): continue
                res.append(elm)
    else:
        for elm in links:
            if (elm[-4:].lower() != '.zip'): continue
            res.append(elm)

    if len(res) == 0: raise Exception('No new files to download were found')

    return res

