#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers

#############################################################################
def create_line(xml_part):
    to_extract = ["country", 
                  "doc-number", 
                  "date"
                 ]

    xml = ET.fromstring(xml_part)

    app_num = xml.find(".//application-reference/document-id/doc-number").text

    parts = xml.findall(".//priority-claims/priority-claim")

    return helpers.extract(parts, to_extract, app_num)
