#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers

#############################################################################
def create_line(xml_part):

    to_extract = [".//patent-properties/patent-property/document-id/doc-number",
                  ".//assignment-record/reel-no",
                  ".//assignment-record/frame-no"
                 ]

    parts_tag = './/patent-assignees'

    sub_tags = ['.//patent-assignee/name',
                './/patent-assignee/city',
                './/patent-assignee/state',
                './/patent-assignee/country-name',
                './/patent-assignee/postcode',
                './/patent-assignee/address-1',
                './/patent-assignee/address-2'
               ]

    return helpers.as_extract(xml_part, to_extract, parts_tag, sub_tags)

