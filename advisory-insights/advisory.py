import logging
import json
import requests
import pyodbc
import azure.functions as func
import datetime

##### Query API #####
##### Single call with client credentials as the basic auth header #####

def query_api(azure_url, oauth_token):
    headers={'Authorization': 'Bearer ' + oauth_token , 'Content-Type': 'application/json'}
    query_response = response = requests.get(azure_url, headers=headers)
    return(query_response)


def get_token(tenant, client_id, client_secret):
    token_url = "https://login.microsoftonline.com/" + tenant + "/oauth2/token"
    data = {'grant_type': 'client_credentials', 'resource': 'https://management.azure.com'}
    access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
    token = json.loads(access_token_response.text)

    return token['access_token']

##### Transform Output of API Query #####
def transform_cm(query_response, date, connection_string):
    f = json.loads(query_response.text)
    transform_output = ""
    for i in f['value']:
        id = i['id']
        aname = i['name']
        rtype = i['type']
        category = i['properties']['category']
        try:
            region = i['properties']['extendedProperties']['RpTenant']
        except:
            region = "N/A"
        severity = i['properties']['impact']
        try:
            impactfield = i['properties']['impactedField']
        except:
            impactfield = "N/A"
        impactvalue = i['properties']['impactedValue']
        lastupdate = i['properties']['lastUpdated']
        recommendedtype = i['properties']['recommendationTypeId']
        resourceId = i['properties']['resourceMetadata']['resourceId'] 
        problem = i['properties']['shortDescription']['problem']
        solution = i['properties']['shortDescription']['solution']
               
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("insert into dbo.ADVISOR(date, NAME, TYPE, CATEGORY, SEVERITY, IMPACTFIELD, IMPACTVALUE, LASTUPDATED, RECOMMENDTYPE, RESOURCEID, PROBLEM, SOLUTION ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", date, aname, rtype, category, severity, impactfield, impactvalue, lastupdate, recommendedtype, resourceId, problem, solution)
        conn.commit()




def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    subscription = req.params.get('subscription')
    tenant = req.params.get('tenant')
    client_id = req.params.get('client_id')
    client_secret = req.params.get('client_secret')
    connection_string = req.params.get('connection_string')

    ##### Generate Variables for Function Run #####
    x = datetime.datetime.now()
    date = x.strftime("%X-%x")
    azure_url = "https://management.azure.com/subscriptions/" + subscription + "/providers/Microsoft.Advisor/recommendations?api-version=2020-01-01"

    ##### Query APi ##### 
    oauth_token = get_token(tenant, client_id, client_secret)
    query_response = query_api(azure_url, oauth_token)
    logging.info('Recommendations Response:', query_response)
    transform_cm(query_response, date, connection_string)
    fileout = json.loads(query_response.text)
    if 'nextLink' in fileout.keys():
        while fileout['nextLink']:
            azure_url = (fileout['nextLink'])
            query_response = query_api(azure_url, oauth_token)
            transform_cm(query_response, date, connection_string)

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
