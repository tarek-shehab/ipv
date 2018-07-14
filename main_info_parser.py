#!/usr/bin/env python
#############################################################################
#
#############################################################################
import xml.dom.minidom
import sys
import time
from multiprocessing import Pool,cpu_count
from gxml_splitter import extract_xml_parts

#############################################################################
def create_line(xml_part):

    rep_line = []
    dom = xml.dom.minidom.parseString(xml_part)

    def extract_append(dom, path, tags_list, output):
        def check_path(path):
            def check_elm(ch_path, start_dom):
                check_point = start_dom.getElementsByTagName(ch_path.pop(0))
                if check_point and len(path) == 0: return check_point
                elif check_point: return check_elm(ch_path, check_point[0])
                else: return False

            if len(path) == 0: return [dom]
            return check_elm(path, dom)

        target_dom = check_path(path)
        if target_dom:
            for child in tags_list:
                baby = target_dom[0].getElementsByTagName(child)
                if baby and hasattr(baby[0].firstChild, "data"): content = baby[0].firstChild.data
                else: content = "-"
                output.append(content)
            return

        output.extend(["-"]*len(tags_list))

    tasks_list = [
                 {"fields": ["country","doc-number","kind","-","-","date"],
                  "path":   ["publication-reference", "document-id"]},
                 {"fields": ["country", "doc-number", "date"],
                  "path":   ["application-reference", "document-id"]},
                 {"fields": ["us-application-series-code",
                             "us-issued-on-continued-prosecution-application",
                             "rule-47-flag",
                             "us-term-extension",
                             "length-of-grant",
                             "invention-title",
                             "number-of-claims",
                             "us-exemplary-claim"],
                 "path": []},
                {"fields": ["variety"],
                 "path":   ["us-botanic"]},
                {"fields": [ "country", "doc-number", "kind", "date"],
                 "path":   ["us-provisional-application", "document-id"]},
                {"fields": [ "doc-number", "kind", "date", "country"],
                 "path":   ["related-publication", "document-id"]},
                ]

    for task in tasks_list:
        extract_append(dom, task["path"], task["fields"], rep_line)

    result = u"\t".join(rep_line).encode('utf-8').strip()+"\n"
#    sys.stdout.write(result+"\r")
    return result

def process(xml_parts,out_file_name):
    of = open("./results/" + out_file_name, "w")
    start_time = time.time()
    pool = Pool(processes = cpu_count()-1 if cpu_count() > 1 else 1)
    results = pool.map(create_line, xml_parts)
    pool.close()
    of.write("".join(results))
    of.close()
    print time.time() - start_time, "sec."

#############################################################################
if __name__ == "__main__":
    process(extract_xml_parts("ipg180102.xml"),"pf_main_info.tsv")


