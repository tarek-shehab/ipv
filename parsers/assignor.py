#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers

#############################################################################
def create_line(xml_part):

    to_extract = [".//assignment-record/reel-no",
                  ".//assignment-record/frame-no",
                  ".//assignment-record/last-update-date/date",
                  ".//assignment-record/purge-indicator",
                  ".//assignment-record/recorded/date",
                  ".//assignment-record/correspondent/name",
                  ".//assignment-record/correspondent/address-1",
                  ".//assignment-record/correspondent/address-2",
                  ".//assignment-record/correspondent/address-3",
                  ".//assignment-record/correspondent/address-4",
                  ".//assignment-record/conveyance-text",
                  ".//patent-assignors/patent-assignor",
                  ".//patent-assignees/patent-assignee"
                 ]

    sub_extract = {
                   "assignor": ["name",
                                "execution-date/date",
                                "address-1",
                                "address-2",
                               ]

                   "assignee": ["name",
                                "address-1",
                                "address-2",
                                "city",
                                "state",
                                "postcode",
                                "country-name"
                               ]
                  }

    xml = ET.fromstring(xml_part)

    res_list = []

    for tag in to_extract:
        tag_suff = tag[tag.rfind('-'):]
        if tag_suf not in ['assignor','assignee']
            ct = xml.find(tag)
            res_list.append(helpers.get_value(ct))
        else:
            ct = xml.findall(tag)
            for c in ct:
                temp_list = res_list[:]
                for tg in sub_extract[tag_suff]
                    elm = c.find(tg)
                    res_list.append(helpers.get_value(elm))

    result = u"\t".join(res_list).encode('utf-8').strip()+"\n"
    return result

