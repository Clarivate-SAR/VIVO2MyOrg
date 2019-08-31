"""
Basic Python client for creating My Organization compatible data from VIVO.
"""
import requests
import logging
import os
import random
import csv
import re
import sys
import argparse
import logging
import time

# Python 2 or 3
PYV = sys.version_info
if PYV > (3, 0):
    from urllib.request import urlopen, Request
    from urllib.parse import urlencode
    from itertools import zip_longest as zipl
else:
    from urllib2 import urlopen, Request
    from urllib import urlencode
    from itertools import izip_longest as zipl


BATCH_SIZE = 50 # Max number of objects to post to My Org at a time
MAX_RETRIES = 10 # The delete all operation takes awhile to complete. How many times to retry before aborting.
MYORG_API_URL = 'https://api.dev-stable.clarivate.com/api/myorg'

# Define the VIVO store
try:
    API_URL = os.environ['VIVO_URL'] + '/api/sparqlQuery'
    UPDATE_URL = os.environ['VIVO_URL'] + '/api/sparqlUpdate'
    EMAIL = os.environ['VIVO_EMAIL'],
    PASSWORD = os.environ['VIVO_PASSWORD']
    NAMESPACE = os.environ['DATA_NAMESPACE']
    MYORG_API_KEY = os.environ['MYORG_API_KEY']
except KeyError:
    raise Exception("Unable to read VIVO credentials in environment variables.")

# Generic query
def vivo_api_query(query, format='application/sparql-results+json'):
    while True:
        payload = {'email': EMAIL, 'password': PASSWORD, 'query': ''+query}
        headers = {'Accept': format}
        logging.debug(query)
        r = requests.post(API_URL, params=payload, headers=headers)
        try:
            r = r.json()
            bindings = r['results']['bindings']
        except ValueError:
            logging.exception(query)
            logging.exception("Nothing returned from query API. "
                              "Ensure your credentials and API url are set "
                              "correctly in your environment variables.")
            bindings = None
        return bindings


# Transform a VIVO URI into a MyOrg compatible ID
def sanitize_id(id):
    id = id.rsplit('/', 1)[1]
    return re.sub('[^A-Za-z0-9]+', '', id)


def delete_all_myorg():
    #
    headers = {'X-ApiKey': MYORG_API_KEY}
    r = session.delete(MYORG_API_URL + '/personorgapubl/deleteall', headers=headers)
    if r.status_code != 204:
        logging.error('API returned status code of {}'.format(r.status_code))
        logging.error(r.text)

    i = 0
    while i < MAX_RETRIES:
        logging.info("Waiting for delete operation to complete...")
        if post_root_org(data): # Primary org
            return True
        time.sleep(30)
        i+=1
    logging.error("Unable to post main organization. This is a fatal error.")
    return False


def post_root_org(data, datatype="organizations"):
    '''
    Root organizations are treated special-like by the API
    '''
    headers = {'X-ApiKey': MYORG_API_KEY}
    logging.debug(data)
    r = session.post(MYORG_API_URL + '/' + datatype, json=data, headers=headers)
    if (r.status_code == 201):
        logging.info("Root organization created successfully")
        return True
    elif (r.status_code == 409):
        if "code" in r.json():
            logging.debug("Waiting... waiting...")
            return False
    else:
        logging.error(r.text)
        raise


def post_to_myorg(data, datatype):
    headers = {'X-ApiKey': MYORG_API_KEY}
    logging.debug(data)
    while True:
        if data is None or len(data) < 1:
            logging.error("No data to post... aborting batch.")
            return False

        r = session.post(MYORG_API_URL + '/' + datatype, json=data, headers=headers)

        try:
            if r.status_code == 201:
                logging.info("Success")
                return True
            elif r.status_code in {200, 204}:
                logging.info("Success or partial success")
                if args.checkresponse:
                    for rec in r.json():
                        if 'error' in rec:
                            logging.warning("Record did not post successfully,"
                            " InCites response: {}".format(rec))
                return True

            else:
                logging.error('API returned status code of {}'.format(r.status_code))
                logging.error(r.text)
                return False

        except AttributeError:
            logging.error("Encountered an error when trying to post data")
            logging.error(r)
            data = r
            for idx, rec in enumerate(data):
                # Remove the troublesome records and retry
                if 'error' in rec:
                    del data[idx]

def update_to_myorg(data, datatype):
    headers = {'X-ApiKey': MYORG_API_KEY}
    logging.debug(data)
    r = session.put(MYORG_API_URL + '/' + datatype + '/' + data['organizationId'], json=data, headers=headers)
    logging.debug(r)
    try:
        if (r.status_code not in {201, 204}):
            logging.error('API returned status code of {}'.format(r.status_code))
            logging.error(r.text)
        else:
            logging.info("Success")
    except AttributeError:
        logging.error(r)

def get_orgs():
    query = '''
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX obo: <http://purl.obolibrary.org/obo/>

            SELECT ?URI ?OrganizationName ?ParentOrgaID
            WHERE {
                ?URI a foaf:Organization .
                ?URI rdfs:label ?OrganizationName .
                OPTIONAL {
                    ?URI obo:BFO_0000050 ?ParentOrgaID
                }
            }
            '''

    r = vivo_api_query(query)
    org_ids = {}
    if r:
        return r
    else:
        return None


def get_people():
    '''
    Expected format for MyOrg API:
    [
      {
        "personId": 725340270,
        "firstName": "string",
        "lastName": "string",
        "email": "string",
        "otherNames": "string",
        "formerOrganization": "string",
        "organizations": [
          {
            "organizationId": 40270007288853
          }
        ]
      }
    ]
    '''

    query1 = '''
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX wos: <http://data.wokinfo.com/ontology/wos#>
            PREFIX vivo: <http://vivoweb.org/ontology/core#>
            PREFIX obo: <http://purl.obolibrary.org/obo/>
            PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
             SELECT ?URI ?FirstName ?LastName ?OrganizationID ?AuthorID
             ?EmailAddress ?OtherNames ?FormerInstitutionWHERE {
                 ?URI a foaf:Person .
                 OPTIONAL { ?URI vivo:orcidId ?AuthorID . }
                 ?URI obo:ARG_2000028 ?vcard .
                 ?vcard vcard:hasName ?name .
                 ?name vcard:givenName ?FirstName .
                 ?name vcard:familyName ?LastName .
                 ?URI vivo:relatedBy ?position .
                 ?position a vivo:Position .
                 ?position vivo:relates ?OrganizationID .
                 ?OrganizationID a foaf:Organization .
                 OPTIONAL {
                 ?vcard vcard:hasEmail ?email .
                 ?email vcard:email ?EmailAddress . }
             }
             '''


    people = []
    bindings = vivo_api_query(query1)

    return bindings



def get_pubs():
    query = '''
            PREFIX bibo: <http://purl.org/ontology/bibo/>
            PREFIX pub: <https://publons.com/ontology/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX wos: <http://data.wokinfo.com/ontology/wos#>
            PREFIX vivo: <http://vivoweb.org/ontology/core#>

            SELECT ?DocumentID ?URI
            WHERE {
             ?pub a bibo:Document .
              ?pub wos:wosId ?DocumentID .
              ?pub vivo:relatedBy ?authorShip .
              ?authorShip vivo:relates ?URI .
              ?URI a foaf:Person .
            }
            '''
    logging.debug(query)
    bindings = vivo_api_query(query)

    return bindings

def sanitize_orgs():
    # Create local IDs since MyOrg only supports alphanumberic IDs and
    # we can't assume the VIVO URIs follow this
    org_ids = {}
    org_ids[args.ORGANIZATION] = 0
    for rec in orgs:
        id = rec['URI']['value']
        name = rec['OrganizationName']['value']
        if 'ParentOrgaID' in rec:
            parents = rec['ParentOrgaID']['value']
        org_ids[id] = sanitize_id(id)


    return org_ids

def sanitize_ids(ids):
    '''
    # Create local IDs since MyOrg only supports alphanumberic IDs and
    # we can't assume the VIVO URIs follow this
    In: {'URI': {'type': 'uri', 'value': 'http://vivo.nih.gov/individual/kathleen-kelly-siebenlist'}}
    Out: {'http://vivo.nih.gov/individual/kathleen-kelly-siebenlist': 'kathleenkellysiebenlist'}
    '''
    sanitized_ids = {}
    for rec in ids:
        key = rec['URI']['value']
        sanitized_ids[key] = sanitize_id(key)
    return sanitized_ids

def prepare_orgs():
    '''
    Format MyOrg API requires
    [
      {
        "organizationId": "string",
        "organizationName": "string",
        "parentId": "string"
      }
    ]
    '''
    orgs_prepared = []
    orgs_noparent = []
    for org in orgs:
        if 'ParentOrgaID' in org:
            parent_id = org_xwalk[org['ParentOrgaID']['value']]
        else:
            parent_id = 0
        org_rec = {"organizationId": org_xwalk[org['URI']['value']],
                   "organizationName": org['OrganizationName']['value'],
                   "parentId": 0}
        org_rec_w_parent = {"organizationId": org_xwalk[org['URI']['value']],
                   "organizationName": org['OrganizationName']['value'],
                   "parentId": parent_id}
        orgs_noparent.append(org_rec)
        orgs_prepared.append(org_rec_w_parent)
    return(orgs_noparent, orgs_prepared)

def prepare_people():
    '''
    Format MyOrg API requires
    [
      {
        "personId": 725340270,
        "firstName": "string",
        "lastName": "string",
        "email": "string",
        "otherNames": "string",
        "formerOrganization": "string",
        "organizations": [
          {
            "organizationId": 40270007288853
          }
        ]
      }
    ]
    '''
    people_prepared = {}
    for person in people:
        person_rec = {"personId": people_xwalk[person['URI']['value']],
                   "firstName": person['FirstName']['value'], "lastName":
                   person['LastName']['value'], "organizations":
                   [{"organizationId": org_xwalk[person['OrganizationID']['value']]}]}
        people_prepared[person['URI']['value']] = person_rec
    return people_prepared

def prepare_pubs():
    '''
    Format MyOrg API requires
    [
      {
        "docId": "string",
        "persons": [
          {
            "personId": "string",
            "organizationId": "string"
          }
        ]
      }
    ]
    '''
    ut_list = {}
    for pub in publications:
        if not pub['DocumentID']['value'].startswith("WOS:"):
            ut = "WOS:{}".format(pub['DocumentID']['value'])
        else:
            ut = pub['DocumentID']['value']

        # Publications aren't normally associated with a specific organization in VIVO
        # so associate with the first one in a person's affiliations
        org = people_dict[pub['URI']['value']]['organizations'][0]['organizationId']

        if ut in ut_list: # Already created record, add person to record
            ut_list[ut]['persons'].append(
              {"personId": people_xwalk[pub['URI']['value']], "organizationId": org})
        else: # New record
            pub_rec = {"docId": ut, "persons":
                       [{"personId": people_xwalk[pub['URI']['value']], "organizationId": org}]
                      }
            ut_list[ut] = pub_rec

    return(ut_list)


def grouper(iterable, n):
    """
    Group iterable into n sized chunks.
    See: https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
    """
    return [iterable[i * n:(i + 1) * n] for i in range((len(iterable) + n - 1) // n )]

def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Set logging "
                        "level to DEBUG.")
    parser.add_argument("--checkresponse", action="store_true", help="Parse "
                        "response from InCites to check for partial success.")
    parser.add_argument('ORGANIZATION', help="Name for the top level "
                        "organization to be added to MyOrg")
    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    data = [{"organizationId": "0", "organizationName": args.ORGANIZATION}]

    orgs = get_orgs()
    org_xwalk = sanitize_ids(orgs)
    org_xwalk[args.ORGANIZATION] = 0
    orgs_noparent, orgs = prepare_orgs()

    people = get_people()
    people_xwalk = sanitize_ids(people)
    people_dict = prepare_people()

    publications = get_pubs()
    pubs_dict = prepare_pubs()

    # Open a session and keep it alive while doing our work
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries = 20)
    session.mount('https://', adapter)

    # Delete all existing data and re-add primary organization
    delete_all_myorg()

    # Can post max 50 orgs at a time. First add without parents, then update.
    for idx, batch in enumerate(grouper(orgs_noparent, BATCH_SIZE)):
        logging.info("Posting org batch {}".format(idx))
        logging.debug("Batch: {}".format(batch))
        post_to_myorg(batch, 'organizations')
        time.sleep(3)
    for rec in orgs: # Update orgs to add correct parent org id
        if rec['parentId'] != 0:
            update_to_myorg(rec, 'organizations')

    # Add people
    people_myorg = []
    for per in people_dict: # Convert from named dictionary to list
        people_myorg.append(people_dict[per])
    for idx, batch in enumerate(grouper(people_myorg, BATCH_SIZE)):
        logging.info("Posting people batch {}".format(idx))
        logging.debug("Batch: {}".format(batch))
        post_to_myorg(batch, 'persons')
        time.sleep(1)


    # Add pubs
    pubs_myorg = []
    for pub in pubs_dict: # Convert from named dictionary to list
        pubs_myorg.append(pubs_dict[pub])
    for idx, batch in enumerate(grouper(pubs_myorg, int(BATCH_SIZE/2))):
        logging.info("Posting pubs batch {}".format(idx))
        logging.debug("Batch: {}".format(batch))
        post_to_myorg(batch, 'publications')
        time.sleep(2)
