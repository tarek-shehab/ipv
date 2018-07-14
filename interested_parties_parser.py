#!/usr/bin/env python
import xml.etree.ElementTree
import xml.dom.minidom
import sys
import time
from multiprocessing import Pool,Value,cpu_count


# "<!DOCTYPE us-patent-grant"
#############################################################################
def extract_xml_parts(xml_file):
    marker = '<?xml version="1.0" encoding="UTF-8"?>\n'
    in_file = open(xml_file, "r")
    res = []
    xmls = []
    i = 0
    start = False
    for line in in_file:
        if line.startswith("<us-patent-grant"):
            if len(res) == 0:
                res.append(line)
                continue
        if line.startswith("</us-patent-grant>"):
            res.append(line)
            res.insert(0, marker)
            elm = "".join(res)
#            create_line(elm)
            xmls.append(elm)
            i+=1
            res = []
            continue
        if len(res) != 0: res.append(line)

    print "Total parts:", i

    return xmls

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

#############################################################################
if __name__ == "__main__":
    t = time.time()
    ddd = extract_xml_parts("ipg180102.xml")
    import xml.etree.ElementTree as ET

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

    i_parties = {"applicant": ".//us-applicants/us-applicant",
                 "inventor": ".//inventors/inventor",
                 "deceased-inventor": ".//inventors/deceased-inventor",
                 "assignee": ".//assignees/assignee"
                }

    for sub in ddd:

        tree = ET.fromstring(sub)
        app_num = tree.find(".//application-reference/document-id/doc-number").text
        for party in i_parties:

            res = tree.findall(i_parties[party])

            for elm in res:
                res_list = [app_num,party]
                for tag in to_extract:
                    ct = elm.find(tag)
                    res_list.append(ct.text if ct is not None else "-")
                print res_list

    quit()
    dom = xml.dom.minidom.parseString(ddd[1])
    app = dom.getElementsByTagName("us-applicants")
    print app
    print app.length
    for elm in app:
        ch =  elm.getElementsByTagName("us-applicant")
        for c in ch:
            res = c.getElementsByTagName("residence")
            print "Residence", res[0].firstChild.data
            print c.childNodes
    print ddd[1]
#    print ddd[0]
#    quit()
#    sys.stdout.write("".join(results))
