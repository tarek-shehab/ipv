######################################################################
#
######################################################################

def get_value(arg):
    return arg.text if (arg is not None and arg.text is not None) else "-"

def extract(parts, to_extract, app_num):

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

def w_extract(parts, to_extract, app_num, add_tag):

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

def cl_extract(parts, to_extract, app_num):

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
                        res_list.extend([value[:value.rfind(' ')].strip(),
                                        value[value.rfind(' '):].strip()])
                    else: res_list.extend(['-', '-'])
                elif tag == "further-classification":
                    temp_list = res_list[:]
                    ct = elm.findall(tag)
                    for c in ct:
                        value = get_value(c)
                        if value != '-':
                            res_list.extend([value[:value.rfind(' ')].strip(),
                                            value[value.rfind(' '):].strip()])
                        else: res_list.extend(['-', '-'])
                        result += u"\t".join(res_list).encode('utf-8').strip()+"\n"
                        res_list = temp_list[:]
        return result

    else: return False
