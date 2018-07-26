######################################################################
#
######################################################################
active_models = {'ipg': [
#                         'main',
#                         'assignee',
#                         'inventor',
#                         'd_inventor',
#                         'applicant',
#                         'claims',
#                         'classification',
#                         'agent',
                          'provisional',
                          'related',
                        ],
#                 'ad': ['assignments', 'a_assignee'],
                 'att': ['attorney']
                }

active_parsers = {'ipg': [
#                         'main',
#                         'assignee',
#                         'inventor',
#                         'd_inventor',
#                         'applicant',
#                         'claims',
#                         'classification',
#                         'agent',
                          'provisional',
                          'related',
                        ],
#                 'ad': ['assignments', 'a_assignee'],
#                 'att': ['attorney']
                }

hdfs_base_dir = '/ipv'