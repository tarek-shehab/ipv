#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import importlib
helpers = importlib.import_module('.helpers', 'parsers')

#############################################################################
def create_line(xml_part):

    to_extract = [".//patent-properties/patent-property/document-id",
                  ".//assignment-record/reel-no",
                  ".//assignment-record/frame-no",
                  ".//assignment-record/last-update-date/date"
                 ]

    parts_tag = './/patent-assignors'

    sub_tags = ['.//patent-assignor/name',
                './/patent-assignor/execution-date/date',
                './/patent-assignor/address-1',
                './/patent-assignor/address-2'
               ]

    return helpers.as_extract(xml_part, to_extract, parts_tag, sub_tags)

