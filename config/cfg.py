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
                 'att': ['attorney']
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
                  'att': ['attorney']
                }

hdfs_base_dir = '/ipv'