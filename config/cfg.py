######################################################################
#
######################################################################
active_models = {'ipg': [
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
                  'ad': [
                         'assignments',
                         'assignee',
                         'assignor'
                          ],
                  'ipa': [
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
                 'att': ['attorney'],
                 'fee_m': ['fee_main'],
                 'fee_d': ['fee_descr']
                }

active_parsers = {'ipg': [
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
                  'pg': [
                          'main',
#                          'assignee',
#                          'inventor',
#                          'd_inventor',
#                          'applicant',
#                          'claims',
#                          'classification',
#                          'agent',
#                          'provisional',
#                          'related',
                        ],
                  'ipa': [
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
                  'ad': [
                         'assignments',
                         'assignee',
                         'assignor'
                        ],
                  'att': ['attorney'],
                  'fee_m': ['fee_main'],
                  'fee_d': ['fee_descr']
                }

hdfs_base_dir = '/ipv'

dwl_links = {'ipg': 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/',
             'pg' : 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/',
             'ipa': 'https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/',
             'ad' : 'https://bulkdata.uspto.gov/data/patent/assignment/',
             'fee': 'https://bulkdata.uspto.gov/data/patent/maintenancefee/MaintFeeEvents.zip'
            }
