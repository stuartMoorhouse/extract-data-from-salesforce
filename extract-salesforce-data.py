import requests
import sys
import json
from xml.etree import ElementTree

import config as config

salesforce_instance = sys.argv[1]


def get_session_id():
    url = 'https://login.salesforce.com/services/Soap/u/v50.0'
    headers = {'SOAPAction': 'login', 'Content-Type': 'text/xml; charset=UTF-8'}
    loginfile = open("login.txt", "r")
    payload = loginfile.read()
    r = requests.post(url, headers=headers, data=payload)
    namespaces = {
        'sforce': 'urn:partner.soap.sforce.com'
    }

    dom = ElementTree.fromstring(r.text)
    sessions_id = dom.findall('*//sforce:sessionId', namespaces)

    return sessions_id[0].text


def get_field_names(obj):
    token = get_session_id()
    url = f'https://{salesforce_instance}.salesforce.com/services/data/v49.0/sobjects/{obj}/describe/'
    auth_value = f"Bearer {token}"
    headers = {'Authorization': auth_value}
    object_description = requests.get(url, headers=headers)
    json_obj_desc = json.loads(object_description.text)

    field_list = json_obj_desc['fields']
    field_names = []
    for f in field_list:
        field_names.append(f['name'])

    return field_names


def object_field_names():
    names = dict()

    for o in config.objects:
       field_names = get_field_names(o)
       names[o] = field_names

    return(names)


def soql_query(object, fields):
    token = get_session_id()
    auth_value = f"Bearer {token}"
    headers = {'Authorization': auth_value}
    url = f'https://{salesforce_instance}.salesforce.com/services/data/v50.0/query/?q=SELECT+{fields}+FROM+{object}+LIMIT+35'

    rows_raw = requests.get(url, headers=headers)
    rows_json = rows_raw.json()
    return rows_json


def values_from_dict_to_csv(dict, keys, sep):
    values_array = []

    for k in keys:
        v = str(dict[k])
        cleaned_v = v.replace('\r\n', '<br/>')
        values_array.append(repr(cleaned_v))

    row = sep.join(values_array)
    return row


def run_queries():
    objects = object_field_names()

    for obj in list(objects.keys()):
        results = []
        results.append("sep=\t")
        attributes = objects[obj]
        attributes_string = ','.join(attributes)
        header = '\t'.join(attributes)
        results.append(header)
        raw_results = soql_query(obj, attributes_string)
        records = raw_results["records"]

        for r in records:
            row = values_from_dict_to_csv(r, attributes, '\t')
            results.append(row)

        results_output = "\n".join(results)
        file_name = f"output/{obj}_records.csv"
        output_file = open(file_name, "w")
        output_file.write(results_output)
        output_file.close()


run_queries()
