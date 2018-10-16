#!/usr/bin/env python
#############################################################################
#
#############################################################################
from time import sleep, time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import routines.tbl_handlers as tbl
import routines.wpr_handlers as parser
import routines.user_agents as ua
from multiprocessing import Manager, Process
from retrying import retry
from datetime import datetime
from datetime import timedelta
from impala.dbapi import connect
from config import cfg
from routines.send_mail import send_mail
import requests
import os.path
from random import uniform, randint, shuffle
from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask
import sys
import argparse

#############################################################################
#
#############################################################################
def wait_between(a,b):
    rand=uniform(a, b) 
    sleep(rand)

#############################################################################
# Execute Impala query
#############################################################################
def run_query(query):
    impala_con = connect(host='localhost')
    impala_cur = impala_con.cursor()
    impala_cur.execute(query)
    result = impala_cur.fetchall()
    impala_cur.close()
    impala_con.close()
    return result

#############################################################################
# Get list of partitions and application Ids
#############################################################################
def get_tasks(year, page_size):

    sql = (
           'SELECT t5.app_id as app_id, t5.proc_date as proc_date FROM '
           '('
           'SELECT app_id, MIN(proc_date) as proc_date FROM '
           '( '
           'SELECT app_id, proc_date from `ipv_db`.grant_main GROUP BY app_id, proc_date '
           'UNION '
           'SELECT app_id, proc_date from `ipv_db`.application_main GROUP BY app_id, proc_date '
           ') t3 '
           'GROUP BY app_id) t5 '
           'LEFT JOIN '
           '(SELECT DISTINCT(app_id) AS app_id from `ipv_db`.tr_history) t1 '
           'ON t5.app_id = t1.app_id '
           'WHERE t1.app_id IS NULL AND SUBSTR(t5.proc_date,1,4) = \'%s\' ') % (year)

    res = list(run_query(sql))
    shuffle(res)
    result = {'size': len(res)}
    if len(res) > page_size:
        start_pos = randint(0,len(res)-page_size)
        part = res[start_pos:start_pos + page_size]
        result['tasks'] = part
    else:
        result['tasks'] = res
    return result

#############################################################################
#
#############################################################################
def load_table(proc_date):
    try:
        impala_con = connect(host=cfg.impala_host)
        impala_cur = impala_con.cursor()
        target_path = ('hdfs://nameservice1/ipv/results/trh_scrape/data%s.tsv') % (proc_date)
        load_sql = ('LOAD DATA INPATH \'%s\' OVERWRITE INTO TABLE `ipv_ext`.`tr_history_new`') % (target_path)
        insert_sql = 'UPSERT INTO TABLE `ipv_db`.`tr_history` SELECT * FROM `ipv_ext`.`tr_history_new`'
        impala_cur.execute(load_sql)
        logging.info('Data has been successfully loaded into temporary table: tr_history!')
        impala_cur.execute(insert_sql)
        logging.info('Data has been successfully loaded into HDFS table: tr_history!')
        impala_cur.close()
        impala_con.close()
        return True
    except Exception as err:
        logging.error('Tables loading failed!')
        logging.error(err)
        return False

#############################################################################
#
#############################################################################
def hdfs_write(proc_date, result):
    hdfs = parser.hdfs_connect()
    hdfs_name = ('/ipv/results/trh_scrape/data%s.tsv') % (proc_date)
    of = hdfs.open(hdfs_name, "wb")
    of.write("".join(result))
    of.close()
    parser.set_impala_permissions('/ipv/results')
#############################################################################
#
#############################################################################
@retry(stop_max_attempt_number=10, wait_random_min=2000, wait_random_max=5000)
def get_captcha(url, site_key):
    try:
        logging.info('Waiting for recaptcha to be solved ...')
        api_key = '3d9e48e7ad1d64de378bc1dea4fd472e'
        client = AnticaptchaClient(api_key)
        task = NoCaptchaTaskProxylessTask(url, site_key)
        job = client.createTask(task)
        job.join()
        return job.get_solution_response()
    except Exception as err:
        logging.info('Failed to resolve captcha: ' + err)
        raise err

#############################################################################
# --year CLI parameter validation
#############################################################################
def check_year(value):
    if int(value) < 2001 and int(value) > 2050:
        raise argparse.ArgumentTypeError("%s is an incorrect procesing year" % value)
    return value

#############################################################################
#
#############################################################################
def scrape_site(app_ids):
    result = []
    st = time()
    logging.info('PublicPair reCaptcha bypass test started')

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument(('user-agent=%s') % (ua.get_user_agent(5)))
    driver = webdriver.Chrome(executable_path='./chromedriver', chrome_options=chrome_options,
                                  service_args=['--verbose', '--log-path=./log/chromedriver.log'])
    driver.set_window_size(1024, 1024)

    target_link = 'https://portal.uspto.gov/pair/PublicPair'

    try:
        driver.get(target_link)
        main_win = driver.current_window_handle

        site_key = driver.find_element_by_class_name('g-recaptcha').get_attribute('data-sitekey')
        data_callback = driver.find_element_by_class_name('g-recaptcha').get_attribute('data-callback')
        logging.info('Site key: {}'.format(site_key))

        g_response_code = get_captcha(target_link, site_key)

        logging.info('Got g-response-code: {}'.format(g_response_code))
        logging.info(('reCaptcha solved in %s sec.') % (round(time()-st,2)))

        javascript_code = 'document.getElementById("g-recaptcha-response").innerHTML = "{}";'.format(g_response_code)
        driver.execute_script(javascript_code)
        logging.info('Code set in page')
        wait_between(1, 2)

        driver.find_element_by_xpath('//input[@type="submit"]').click()

        logging.info('Page has been submitted')

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH ,'//input[@id="number_id"]')))
        except:
            raise('Failed to load starting page')

        for elm in app_ids:
            try:
                app_id_input_field = driver.find_element_by_xpath('//input[@id="number_id"]')
                search_button = driver.find_element_by_xpath('//input[@id="SubmitPAIR"]')
                app_id_input_field.send_keys(str(elm[0]))
                search_button.click()

                logging.info(('Application ID: %s has been sent') % (str(elm[0])))

                tr_history_tab = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH ,'//a[@tabindex="12"]')))

#                tr_history_tab = driver.find_element_by_xpath('//a[@tabindex="12"]')

                tr_history_tab.click()

                logging.info('Switch to transaction history tab')

                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID ,'pagedescription')))

                table = driver.find_elements_by_xpath('//tr[@class="wpsTableNrmRow" or @class="wpsTableShdRow"]/td')

                for i in [ n for n in range(len(table)) if n%2 == 0]:
                    line = ['-']*6
                    r_d = (table[i].text).split('-')
                    line[0] = str(elm[0])
                    line[1] = '-'.join([r_d[2],r_d[0],r_d[1]]) + ' 00:00:00'
                    line[3] = table[i+1].text
                    line[4] = '1'
                    line[5] = str(elm[1])
                    result.append(line)
                logging.info(('%s records has been extracted for App ID: %s') % (str(len(table)//2), str(elm[0])))

                main_tab = driver.find_element_by_xpath('//a[@tabindex="6"]')
                main_tab.click()
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH ,'//input[@id="number_id"]')))

                logging.info('Switch to main tab')
            except Exception as err:
                logging.error(('Failed to extract data for App ID: %s') % (str(elm[0])))
                err = str(err).strip()[8:]
                if err == '':
                    message = 'No transaction history info for this App ID!'
                elif 'Unable to locate element' in err:
                    message = 'PublicPair service did not respond or page is stuck!'
                else:
                    message = err
                logging.error(message)
                continue

#        driver.save_screenshot('./shot.png')
    except Exception as err:
        logging.error('Failed to process!')
        err = str(err).strip()[8:]
        if 'Unable to locate element' in err:
            message = 'PublicPair service did not respond or page is stuck!'
        else:
            message = err
        logging.error(message)
    finally:
        driver.quit()
        logging.info(('reCaptcha bypass test finished in %s sec.') % (round(time()-st,2)))
        return result if result else False


#############################################################################
if __name__ == "__main__":
    descr = ('Transaction history screper v0.1\n'
             'Author: Alex Kovalsky')
    arg_parser = argparse.ArgumentParser(description=descr)
    arg_parser.add_argument("--year", type=check_year,
            default = datetime.now().year,
            help="Target year")
    arg_parser.add_argument("--block_size", type=int,
            default = 50,
            help="App IDs block size (default = 50)")
    arg_parser.add_argument("--retries", type=int,
            default = 2,
            help="Numbers of retries for App IDs block (default = 2)")

    if len(sys.argv) == 1:
        arg_parser.print_help()
        sys.exit(1)

    args = arg_parser.parse_args()
    year = args.year

    log_file_name = ('./log/nocaptcha/scrap_%s.log') % (str(datetime.now())[2:19].replace(' ','_'))

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s',
                        level=logging.INFO)

    logFormatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler(log_file_name)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)


    portion = get_tasks(year, args.block_size)

    logging.info(('Total numbers of App Ids for year: %s, is: %s') % (year, portion['size']))

    enough = False
    start_amount = portion['size']
    total_amount = portion['size']
    retries = 0
    total_ids = 0
    parser.set_env()

    while not enough:
        logging.info(('Got %s App IDs to extract transaction history info') % (len(portion['tasks'])))

        res = scrape_site(portion['tasks'])
        ids = set()
        result = []
        if res:
            for ln in res: 
                ids.add(ln[0])
                ln[-1] = ln[-1]
                result.append(u"\t".join(ln).encode('utf-8').strip()+"\n")

            marker = str(int(time()))
            hdfs_write(marker, result)
            load_table(marker)

        total_ids += len(ids)
        logging.info(('STAT: IDs extracted during current run    : %s') % (str(len(ids))))
        logging.info(('STAT: IDs extracted during current session: %s') % (str(total_ids)))
        logging.info(('STAT: IDs remaining                       : %s') % (str(total_amount - total_ids)))
        portion = get_tasks(year, args.block_size)

        if start_amount == portion['size']:
            retries += 1
        else:
            retries = 0

        start_amount = portion['size']

        if portion['size'] == 0 or retries == args.retries:
            enough = True
            logging.info('Processing finised')
            message = 'All possible App IDs has been extracted!' if portion['size'] == 0 else 'Retries limit exceeded!'
            logging.info(message)
