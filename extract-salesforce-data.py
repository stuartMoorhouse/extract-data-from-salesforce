import requests
import sys
import json
from xml.etree import ElementTree

import config as config

salesforce_instance = sys.argv[1]



# Get a session ID to authenticate the requests
def getSessionId():
    url = 'https://login.salesforce.com/services/Soap/u/v50.0'
    headers = {'SOAPAction': 'login', 'Content-Type': 'text/xml; charset=UTF-8'}
    loginfile = open("login.txt", "r")
    payload = loginfile.read()
    r = requests.post(url, headers=headers, data=payload)

    namespaces = {
        'sforce': 'urn:partner.soap.sforce.com'
    }
    dom = ElementTree.fromstring(r.text)
    sessionsid = dom.findall('*//sforce:sessionId', namespaces)

    return sessionsid[0].text

# Get a list of its fields
def getFieldNames(obj):
    token = getSessionId()
    url = f'https://{salesforce_instance}.salesforce.com/services/data/v49.0/sobjects/{obj}/describe/'
    authvalue = f"Bearer {token}"
    headers = {'Authorization': authvalue}
    object_description = requests.get(url, headers=headers)
    json_obj_desc = json.loads(object_description.text)
    field_list = json_obj_desc['fields']
    field_names = []
    for f in field_list:
        field_names.append(f['name'])
    return field_names
    # print(field_names)

# Create object/ field names dictionary
def objectFieldNames():
    object_field_names = dict()
    for o in config.objects:
       field_names = getFieldNames(o)
       object_field_names[o] = field_names

    # return(object_field_names)
    print(object_field_names)


# def generate_sfql_queries():
#    field_names_by_object = objectFieldNames()

def soql_query(object, fields):
    token = getSessionId()
    authvalue = f"Bearer {token}"
    headers = {'Authorization': authvalue}

    url = f'https://{salesforce_instance}.salesforce.com/services/data/v20.0/query/?q=SELECT+{fields}+from+{object}'
    rows = requests.get(url, headers=headers)
    return(rows.text)

f = soql_query('Opportunity', 'Id,IsDeleted,AccountId')


soql_results = open('soqlresults.txt', 'a+')
soql_results.write(f)
soql_results.close()



# objectFieldNames()
# objectFieldNames()
# Create a SOQL query

# Get datas






