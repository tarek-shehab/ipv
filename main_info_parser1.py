#!/usr/bin/env python
#############################################################################
#
#############################################################################
import xml.dom.minidom
import xml.etree.ElementTree as ET
import sys
import time
from multiprocessing import Pool,cpu_count
from gxml_splitter import extract_xml_parts

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

    tree = ET.fromstring(xml_part)

    res_list = []

    for tag in to_extract:
        ct = tree.find(tag)
        res_list.append(ct.text if (ct is not None and ct.text is not None) else "-")

    result = u"\t".join(res_list).encode('utf-8').strip()+"\n"
    return result

def process(xml_parts,out_file_name):
    of = open("./results/" + out_file_name, "w")
    start_time = time.time()
    pool = Pool(processes = cpu_count()-1 if cpu_count() > 1 else 1)
    results = pool.map(create_line, xml_parts)
    pool.close()
    of.write("".join(results))
    of.close()
    print time.time() - start_time, "sec. has been spent to processing"

#############################################################################
if __name__ == "__main__":
#    dom = extract_xml_parts("ipg180102.xml")
#    for lm in dom:
#        print create_line(lm)

    process(extract_xml_parts("ipg180102.xml"),"pf_main_info1.tsv")


