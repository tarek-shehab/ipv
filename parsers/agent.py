#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers

#############################################################################
def create_line(xml_part):
    to_extract = ["addressbook/orgname",
                  "addressbook/address/country",
                  "addressbook/first-name",
                  "addressbook/last-name"
                 ]

    app_num_tag = ".//application-reference/document-id/doc-number"

    parts_tag = ".//agents/agent"

    return helpers.w_extract(xml_part, to_extract, parts_tag, app_num_tag, 'rep-type')
