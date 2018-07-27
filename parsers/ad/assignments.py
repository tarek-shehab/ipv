#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import importlib
helpers = importlib.import_module('.helpers', 'parsers')


#############################################################################
def create_line(xml_part):

    to_extract = [".//patent-properties/patent-property/document-id/doc-number",
                  ".//assignment-record/reel-no",
                  ".//assignment-record/frame-no",
                  ".//assignment-record/last-update-date/date",
                  ".//assignment-record/purge-indicator",
                  ".//assignment-record/recorded-date/date",
                  ".//assignment-record/correspondent/name",
                  ".//assignment-record/correspondent/address-1",
                  ".//assignment-record/correspondent/address-2",
                  ".//assignment-record/correspondent/address-3",
                  ".//assignment-record/correspondent/address-4",
                  ".//assignment-record/conveyance-text"
                 ]
    xml = ET.fromstring(xml_part)

    res_list = []

    for tag in to_extract:
        ct = xml.find(tag)
        res_list.append(ct.text if (ct is not None and ct.text is not None) else "-")

    result = u"\t".join(res_list).encode('utf-8').strip()+"\n"
    return result

