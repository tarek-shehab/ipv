######################################################################
# Active models describe the model files used by processing routines
# to create particular Impala table structures
######################################################################
active_models = {
    'ipg': [
             'main',
             'assignee',
             'inventor',
             'd_inventor',
             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'ad':   [
             'assignments',
             'assignee',
             'assignor'
            ],
    'pa':   [
             'main',
             'assignee',
             'inventor',
#             'd_inventor',
#             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'pg':   [
             'main',
             'assignee',
             'inventor',
#             'd_inventor',
#             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'ipa':  [
             'main',
             'assignee',
             'inventor',
             'd_inventor',
             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'thist':['transactions'],
    'ainf' :['add_info'],
    'att'  :['attorney'],
    'fee_m':['fee_main'],
    'fee_d':['fee_descr'],
    'phi'  :['ph_info']
    }

######################################################################
# Active parsers describe the parsers files used by processing routines
# to parse particular parts of XML or TXT source files
######################################################################
active_parsers = {
    'ipg':  [
             'main',
             'assignee',
             'inventor',
             'd_inventor',
             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'pg':   [
             'main',
             'assignee',
             'inventor',
#             'd_inventor',
#             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'pa':   [
             'main',
             'assignee',
             'inventor',
#             'd_inventor',
#             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'ipa':  [
             'main',
             'assignee',
             'inventor',
             'd_inventor',
             'applicant',
             'claims',
             'classification',
             'agent',
             'provisional',
             'related',
            ],
    'ad':   [
             'assignments',
             'assignee',
             'assignor'
            ],
    'att':  ['attorney'],
    'fee_m':['fee_main'],
    'fee_d':['fee_descr']
    }

######################################################################
# Base HDFS directory
######################################################################
hdfs_base_dir = '/ipv'

######################################################################
# Base links for downloading source files
######################################################################
dwl_links = {
    'ipg': 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/',
    'pg' : 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/',
    'pa' : 'https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/',
    'ipa': 'https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/',
    'ad' : 'https://bulkdata.uspto.gov/data/patent/assignment/',
    'fee': 'https://bulkdata.uspto.gov/data/patent/maintenancefee/MaintFeeEvents.zip',
    'att': 'http://www.uspto.gov/attorney-roster/attorney.zip'
    }

######################################################################
# Impala entry point host
######################################################################
impala_host = '192.168.250.11'

######################################################################
# Mail credentials and parameters used by processing routines
# for sending notifications
######################################################################
mail_params =   {
    'server'   : 'mail.gandi.net',
    'username' : 'reports@taikitech.com',
    'password' : 'reportdaemon',
    'send_from': 'reports@taikitech.com',
#    'send_to'  : ['support@taikitech.com'],
    'send_to'  : ['support@taikitech.com', 'tarek.shehab@ipvisibility.com'],
#    'text'     : text,
#    'subject'  : ('%s file %s report') % (type_map[ftype], mode_map[mode]),
#    'files'    : [out_file_xlsx]
    }
