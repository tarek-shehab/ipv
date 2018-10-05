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
from random import uniform, randint
from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask

#############################################################################
#
#############################################################################
def wait_between(a,b):
    rand=uniform(a, b) 
    sleep(rand)

#############################################################################
#
#############################################################################
def browser_test_invisible():
    app_ids = [
        9342866,
        9725011,
        9725025,
        9725131,
        9725153,
        9725183,
        9725253,
        9725320,
        9725378,
        9725385]
    st = time()
    print '[=] PublicPair reCaptcha bypass test started [=]'

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

        # get sitekey from page

        site_key = driver.find_element_by_class_name('g-recaptcha').get_attribute('data-sitekey')
        data_callback = driver.find_element_by_class_name('g-recaptcha').get_attribute('data-callback')
        print '[+] Site key: {}'.format(site_key)
#        print '[+] Callback method: {}'.format(data_callback)

        # complete captcha
        print '[+] Waiting for recaptcha to be solved ...'

        api_key = '3d9e48e7ad1d64de378bc1dea4fd472e'
        url = "https://portal.uspto.gov/pair/PublicPair"

        client = AnticaptchaClient(api_key)
        task = NoCaptchaTaskProxylessTask(url, site_key)
        job = client.createTask(task)
        job.join()

        g_response_code = job.get_solution_response()

        print '[+] Got g-response-code: {}'.format(g_response_code) # we got it
        print ('[=] reCaptcha solved in %s sec. [=]') % (round(time()-st,2))

        javascript_code = 'document.getElementById("g-recaptcha-response").innerHTML = "{}";'.format(g_response_code)
        driver.execute_script(javascript_code)       # set g-response-code in page (invisible to the 'naked' eye)
        print '[+] Code set in page'
        wait_between(1, 2)
        # submit form
        driver.find_element_by_xpath('//input[@type="submit"]').click()

        print '[+] Page has been submitted'
#        wait_between(2.5, 3.7)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH ,'//input[@id="number_id"]')))

        for id in app_ids:
            app_id_input_field = driver.find_element_by_xpath('//input[@id="number_id"]')
            search_button = driver.find_element_by_xpath('//input[@id="SubmitPAIR"]')
            app_id_input_field.send_keys(str(id))
            search_button.click()
#            wait_between(0.5, 1.5)
            print '[+] Application ID has been sent'

            tr_history_tab = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH ,'//a[@tabindex="12"]')))

#            tr_history_tab = driver.find_element_by_xpath('//a[@tabindex="12"]')

            tr_history_tab.click()

#            wait_between(0.5, 1.5)
            print '[+] Switch to transaction history tab'

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID ,'pagedescription')))

            table = driver.find_elements_by_xpath('//tr[@class="wpsTableNrmRow" or @class="wpsTableShdRow"]/td')
            for i in [ n for n in range(len(table)) if n%2 == 0]:
                print table[i].text+'\t'+table[i+1].text

            main_tab = driver.find_element_by_xpath('//a[@tabindex="6"]')
            main_tab.click()
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH ,'//input[@id="number_id"]')))

            print '[+] Switch to main tab'

        driver.save_screenshot('./shot.png')

#        sleep(10)
    finally:
        driver.quit()        # quit browser
        print ('[=] reCaptcha bypass test finished in %s sec. [=]') % (round(time()-st,2))


#############################################################################
if __name__ == "__main__":

    browser_test_invisible()
