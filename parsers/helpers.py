######################################################################
#
######################################################################
def extract(parts, to_extract, app_num):

    if len(parts) != 0:
        result = ''
        for elm in parts:
            res_list = [app_num]
            for tag in to_extract:
                ct = elm.find(tag)
                res_list.append(ct.text if (ct is not None and ct.text is not None) else "-")
            result += u"\t".join(res_list).encode('utf-8').strip()+"\n"
        return result

    else: return False
