#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers

#############################################################################


def create_line(xml_part):
    to_extract = ["residence/country", 
                  "nationality/country", 
                  "addressbook/orgname",
                  "addressbook/address/city",
                  "addressbook/address/country",
                  "addressbook/address/state",
                  "addressbook/first-name",
                  "addressbook/last-name",
                  "addressbook/role"
                 ]

    xml = ET.fromstring(xml_part)

    app_num = xml.find(".//application-reference/document-id/doc-number").text

    parts = xml.findall(".//assignees/assignee")

    return helpers.extract(parts, to_extract, app_num)
