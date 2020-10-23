import requests
import sys
import json
from xml.etree import ElementTree

import config as config

salesforce_instance = sys.argv[1]
output_format = sys.argv[2]

if not (output_format == "csv" or output_format == "json"):
    print('please choose "csv" or "json" output as the second argument')
    sys.exit(1)


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

    return names


def soql_query(query_string):

    token = get_session_id()
    auth_value = f"Bearer {token}"
    headers = {'Authorization': auth_value}
    url = f'https://{salesforce_instance}.salesforce.com/{query_string}'

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


def process_results(object, attributes, attributes_string, query_string, count):
    raw_results = soql_query(query_string)
    records = raw_results["records"]

    if output_format == "csv":
        results = []
        results.append("sep=\t")
        header = '\t'.join(attributes)
        results.append(header)

        for r in records:
            row = values_from_dict_to_csv(r, attributes, '\t')
            results.append(row)

        results_output = "\n".join(results)
        file_name = f"output/{object}_records_{count}.csv"
        output_file = open(file_name, "w")
        output_file.write(results_output)
        output_file.close()

    elif output_format == "json":
        for r in records:
            identifier = r["Id"]
            file_name = f"output/{object}_{identifier}.json"
            output_file = open(file_name, "w")
            output_file.write(json.dumps(r))

    if "nextRecordsUrl" in raw_results:
        process_results(object, attributes, attributes_string, raw_results["nextRecordsUrl"], count + 1)


def run_queries():
    objs = object_field_names()

    for obj in list(objs.keys()):
        atts = objs[obj]
        atts_string = ','.join(atts)
        query_string = f"""services/data/v50.0/query/?q=SELECT+{atts_string}+FROM+{obj}+LIMIT+350"""
        process_results(obj, atts, atts_string, query_string, 1)

run_queries()
