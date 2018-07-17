import os
import logging
import re
#############################################################################
# Split Patent Grant XML file to list of XML
#############################################################################
def extract_xml_parts(xml_file):
    if not xml_file:
        logging.error(('Incorrect argument for XML splitter') % (name))
        return False

    name = os.path.basename(xml_file)
    dig = re.search('\d', name)
    f_type = name[:dig.start()] if dig else False

    if f_type == 'ipg':
        open_tag = "<us-patent-grant"
        close_tag = "</us-patent-grant>"
    elif f_type == 'ipa':
        open_tag = "<patent-assignment"
        close_tag = "</patent-assignment>"
    else:
        logging.error(('Can\'t split file <%s>') % (name))
        return False

    marker = '<?xml version="1.0" encoding="UTF-8"?>\n'
    in_file = open(xml_file, "r")
    res = []
    xmls = []
    i = 0
    for line in in_file:
        if line.startswith(open_tag):
            if len(res) == 0:
                res.append(line)
                continue
        if line.startswith(close_tag):
            res.append(line)
            res.insert(0, marker)
            elm = "".join(res)
            xmls.append(elm)
            i+=1
            res = []
            continue
        if len(res) != 0: res.append(line)

#    print "Total parts:", i

    return xmls


