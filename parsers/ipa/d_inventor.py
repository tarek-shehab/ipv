#############################################################################
# Extract deceased inventor data from Applicaton XML file
#############################################################################
import xml.etree.ElementTree as ET
import importlib
helpers = importlib.import_module('.helpers', 'parsers')

#############################################################################
def create_line(xml_part):
    to_extract = [
                  ".//first-name",
                  ".//last-name",
                  ".//address/city",
                  ".//address/country"
                 ]

    app_num_tag = ".//application-reference/document-id/doc-number"

    parts_tag = ".//inventors/deceased-inventor"

    return helpers.extract(xml_part, to_extract, parts_tag, app_num_tag)
