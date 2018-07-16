#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers

#############################################################################
def create_line(xml_part):
    to_extract = ["country", 
                  "main-classification",
                  "further-classification"
                 ]

    xml = ET.fromstring(xml_part)

    app_num = xml.find(".//application-reference/document-id/doc-number").text

    parts = xml.findall(".//classification-national")

    return helpers.cl_extract(parts, to_extract, app_num)
