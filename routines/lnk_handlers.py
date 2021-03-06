#############################################################################
# Get links handlers
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

#############################################################################
# Get list of allowed file types
#############################################################################
def get_possible_ftypes():
   return [t if not t.startswith('fee') else 'fee' for t in cfg.active_parsers.keys()]

#############################################################################
# Get full links list from paricular web page (depending of file type)
#############################################################################
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

    return res if len(res) > 0 else False

#############################################################################
# Get full links list from paricular web page (depending of file type) TOR
#############################################################################
def get_links_list_(year, ftype):
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
    session = requests.session()
    session.proxies = {}
    session.proxies['http'] = 'socks5h://localhost:9050'
    session.proxies['https'] = 'socks5h://localhost:9050'

    page = session.get(url, headers=HEADERS)

    content = html.fromstring(page.content)

    file_list =  content.xpath('.//tr/td/a/text()')

    for fl in file_list:
        if fl.endswith('.zip') and (fl.split('/')[-1].find('-') < 0):
            if ftype == 'ad':
                res.append(durl_prefix + fl)
            elif ftype in ['pg','pa']: res.append(durl_prefix + fl[2:4] + '/' + fl)
            else:res.append(durl_prefix + fl[3:5] + '/' + fl)

    return res if len(res) > 0 else False

#############################################################################
# Get allowed to download links list (depending of file type)
#############################################################################
def get_links(year, ftype, full_list=None, tor=None):

    if ftype not in get_possible_ftypes(): raise Exception('Incorrect file type!')
    if tor: links = get_links_list_(year, ftype)
    else:
        links = get_links_list(year, ftype)
    if not links: raise Exception('No links were extracted for this year')
    res = []
    if not full_list:
        tbl_preffix = {'ipg' :'grant',
                       'ipa' :'application',
                        'ad' :'assignment',
                        'att':'attorney',
                        'fee':'fee'}[ftype]

        impala_con = connect(host=cfg.impala_host)
        impala_cur = impala_con.cursor()
        if ftype in ['ipg', 'ipa', 'pg', 'pa', 'fee']:
            query = ('SELECT DISTINCT proc_date FROM `ipv_db`.`%s_main` '
                     'WHERE SUBSTR(proc_date,1,4) = \'%s\' ORDER by proc_date DESC') % (tbl_preffix, year)
        elif ftype in ['att']:
            query = ('SELECT updated FROM `ipv_db`.`%s` LIMIT 1') % (tbl_preffix)
        else:
            query = ('SELECT DISTINCT last_update FROM `ipv_db`.`%s_main` '
                     'WHERE SUBSTR(last_update,1,4) = \'%s\' ORDER BY last_update DESC') % (tbl_preffix, year)

        impala_cur.execute(query)
        dates = [elm[0][2:] for elm in impala_cur.fetchall()]
        impala_cur.close()
        impala_con.close()

        if ftype in ['fee','att']:
            meta = urllib.urlopen(links[0]).info()
            file_date = datetime.strptime(meta.getheaders('Last-Modified')[0],'%a, %d %b %Y %H:%M:%S %Z')
            base_date = datetime.strptime('20' + dates[0], '%Y%m%d')
            if (file_date-base_date).days > 2: res = links[:]
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

