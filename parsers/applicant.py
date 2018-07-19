#############################################################################
#
#############################################################################
import xml.etree.ElementTree as ET
import helpers

#############################################################################
def create_line(xml_part):
    to_extract = ["residence/country", 
                  "nationality/country", 
                  "addressbook/orgname",
                  "addressbook/address/city",
                  "addressbook/address/country",
                  "addressbook/address/state",
                  "addressbook/first-name",
                  "addressbook/last-name",
#                  "addressbook/role"
                 ]

    app_num_tag = ".//application-reference/document-id/doc-number"

    parts_tag = ".//us-applicants/us-applicant"

    return helpers.extract(xml_part, to_extract, parts_tag, app_num_tag)
