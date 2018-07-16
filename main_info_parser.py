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

import subprocess
import sys
import os
import subprocess
from subprocess import PIPE
from StringIO import StringIO

def set_env():
    # libhdfs.so path
    cmd = ["locate", "-l", "1", "libhdfs.so"]
    libhdfsso_path = subprocess\
        .Popen(cmd, stdout=PIPE)\
        .stdout\
        .read()\
        .rstrip()
    os.environ["ARROW_LIBHDFS_DIR"] = os.path.dirname(libhdfsso_path)
#    sys.stderr.write("Set ARROW_LIBHDFS_DIR: %s\n"
#        % (os.environ["ARROW_LIBHDFS_DIR"]))

    # JAVA_HOME path
    os.environ["JAVA_HOME"] = '/usr/lib/jvm/java-7-oracle-cloudera'
#    sys.stderr.write("Set JAVA_HOME: %s\n"
#        % (os.environ["JAVA_HOME"]))

    # classpath
    cmd = ["/usr/bin/hadoop", "classpath", "--glob"]
    hadoop_cp = subprocess\
        .Popen(cmd, stdout=PIPE)\
        .stdout\
        .read()\
        .rstrip()
    if "CLASSPATH" in os.environ:
        os.environ["CLASSPATH"] = os.environ["CLASSPATH"] + ":" + hadoop_cp
    else:
        os.environ["CLASSPATH"] = hadoop_cp
#    sys.stderr.write("Set CLASSPATH: %s\n"
#        % (os.environ["CLASSPATH"]))



#############################################################################
if __name__ == "__main__":


#    hdfs_file =  Hfile('localhost, 8020, ''/ipv/results/main/data180201.tsv', mode='w')
#    content = "The quick brown fox jump over lazy dog!\t"
#    hdfs_file.write(content)
#    hdfs_file.close()

    sys.stdout = os.devnull
    sys.stderr = os.devnull

    set_env()
    import pyarrow as pa


    hdfs = pa.hdfs.connect("192.168.250.15", 8020, user='hdfs', driver='libhdfs')
    content = "The quick brown fox jump over lazy dog!\n"
    hdfs_file =  hdfs.open('/ipv/results/main/data180201.tsv', "wb")


    hdfs_file.write(content)
    hdfs_file.close()
    hdfs.close()

    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__



# Using libhdfs3
#hdfs_alt = HdfsClient(host, port, username, driver='libhdfs3')

#    hdfs_file =  hdfs.open('/ipv/results/main/data180201.tsv', "w")

#    content = "The quick brown fox jump over lazy dog!\t"

#    hdfs_file.write(content)
#    hdfs_file.close()
