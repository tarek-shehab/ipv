import xml.etree.ElementTree as ET
import logging

######################################################################
#
######################################################################
#def get_value(arg):
#    return arg.text if (arg is not None and arg.text is not None) else "-"

def get_value(arg):
    if arg is not None and len(arg) > 0:
        res = []
        for elm in arg:
            head = elm.text if (elm is not None and elm.text is not None) else "-"
            tail = elm.tail if (elm is not None and elm.tail is not None) else "-"
            res.extend([head,tail])
        return ' '.join(res)
    else: return arg.text if (arg is not None and arg.text is not None) else "-"



def get_parts(xml_part, parts_tag, app_num_tag):

    xml = ET.fromstring(xml_part)

    app_num = xml.find(app_num_tag).text

    parts = xml.findall(parts_tag)

    return [parts, app_num]


def extract(xml_part, to_extract, parts_tag, app_num_tag):
    try:
        args = get_parts(xml_part, parts_tag, app_num_tag)
        parts = args[0]
        app_num = args[1]
        if len(parts) != 0:
            result = ''
            for elm in parts:
                res_list = [app_num]
                for tag in to_extract:
                    ct = elm.find(tag)
                    res_list.append(get_value(ct))
                result += u"\t".join(res_list).encode('utf-8').strip()+"\n"
            return result

        else: return False
    except Exception as err:
        logging.error('General parser error!')
        logging.error(err)
        return False

def w_extract(xml_part, to_extract, parts_tag, app_num_tag, add_tag):
    try:
        args = get_parts(xml_part, parts_tag, app_num_tag)
        parts = args[0]
        app_num = args[1]

        if len(parts) != 0:
            result = ''
            for elm in parts:
                res_list = [app_num, elm.get(add_tag)]
                for tag in to_extract:
                    ct = elm.find(tag)
                    res_list.append(get_value(ct))
                result += u"\t".join(res_list).encode('utf-8').strip()+"\n"
            return result

        else: return False
    except Exception as err:
        logging.error('W-parser error!')
        logging.error(err)
        return False

def get_class(arg):

    tmp = arg.replace(' ','0')

    sbc = tmp[3:] if len(tmp[3:]) <= 3 else tmp[3:6] + '.' + tmp[6:] + '0'*(3-len(tmp[6:]))

    fpt = tmp[:3]
    return [fpt if fpt[0] != '0' else fpt[1:], sbc]

def cl_extract(xml_part, to_extract, parts_tag, app_num_tag):
    try:
        args = get_parts(xml_part, parts_tag, app_num_tag)
        parts = args[0]
        app_num = args[1]
        if len(parts) != 0:
            result = ''
            for elm in parts:
                res_list = [app_num]
                for tag in to_extract:
                    ct = elm.find(tag)
                    if tag == "country":
                        res_list.append(get_value(ct))
                    elif tag == "main-classification":
                        value = get_value(ct)
                        if value != '-':
                            res_list.extend(get_class(value))
                        else: res_list.extend(['-', '-'])
                    elif tag == "further-classification":
                        temp_list = res_list[:]
                        ct = elm.findall(tag)
                        if len(ct) != 0:
                            for c in ct:
                                value = get_value(c)
                                if value != '-':
                                    res_list.extend(get_class(value))
                                else: res_list.extend(['-', '-'])
                                result += u"\t".join(res_list).encode('utf-8').strip()+"\n"
                                res_list = temp_list[:]

                        else:
                             res_list.extend(['-', '-'])
                             result += u"\t".join(res_list).encode('utf-8').strip()+"\n"
            return result

        else: return False
    except Exception as err:
        logging.error('CL-parser error!')
        logging.error(err)
        return False

def as_extract(xml_part, to_extract, parts_tag, sub_tags):
    try:
        xml = ET.fromstring(xml_part)
        parts = xml.findall(parts_tag)
        res_list = []
        if len(parts) != 0:
            result = ''
            for tag in to_extract:
                ct = xml.find(tag)
                res_list.append(get_value(ct))
            temp_list = res_list[:]
            for elm in parts:
                for tag in sub_tags:
                    ct = elm.find(tag)
                    res_list.append(get_value(ct))

                result += u"\t".join(res_list).encode('utf-8').strip()+"\n"
                res_list = temp_list[:]
            return result

        else: return False
    except Exception as err:
        logging.error('AS-parser error!')
        logging.error(err)
        return False
