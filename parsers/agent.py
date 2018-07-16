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

    xml = ET.fromstring(xml_part)

    app_num = xml.find(".//application-reference/document-id/doc-number").text

    parts = xml.findall(".//agents/agent")

    return helpers.w_extract(parts, to_extract, app_num, 'rep-type')
