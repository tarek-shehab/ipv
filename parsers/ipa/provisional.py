#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers
#############################################################################
def create_line(xml_part):

    to_extract = [".//application-reference/document-id/doc-number",
                  ".//publication-reference/document-id/country",
                  ".//publication-reference/document-id/doc-number",
                  ".//publication-reference/document-id/kind",
                  ".//publication-reference/document-id/date",
                  ".//application-reference/document-id/country",
                  ".//application-reference/document-id/date",
                  ".//us-application-series-code",
                  ".//us-issued-on-continued-prosecution-application",
                  ".//rule-47-flag",
                  ".//us-term-extension",
                  ".//length-of-grant",
                  ".//invention-title",
                  ".//number-of-claims",
                  ".//us-exemplary-claim",
                  ".//us-botanic/variety",
                  ".//us-provisional-application/document-id/country",
                  ".//us-provisional-application/document-id/doc-number",
                  ".//us-provisional-application/document-id/kind",
                  ".//us-provisional-application/document-id/date",
                  ".//related-publication/document-id/country",
                  ".//related-publication/document-id/doc-number",
                  ".//related-publication/document-id/kind",
                  ".//related-publication/document-id/date"
                 ]

    xml = ET.fromstring(xml_part)

    res_list = []

    for tag in to_extract:
        ct = xml.find(tag)
        res_list.append(helpers.get_value(ct))

    result = u"\t".join(res_list).encode('utf-8').strip()+"\n"
    return result

