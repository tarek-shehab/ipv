#!/usr/bin/env python
#############################################################################
# Split Patent Grant XML file to list of XML
#############################################################################
def extract_xml_parts(xml_file):
    marker = '<?xml version="1.0" encoding="UTF-8"?>\n'
    in_file = open(xml_file, "r")
    res = []
    xmls = []
    i = 0
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

#    print "Total parts:", i

    return xmls

