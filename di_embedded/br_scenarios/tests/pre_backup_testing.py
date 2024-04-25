import os
import logging
import shutil
import requests
import json
import time
import sys

session = requests.Session()
headers = {
        "Content-Type": "application/json",
        'accept': 'application/json',
        'X-Requested-With': 'Fetch',
        "x-sap-boc-user-properties-b64": "eyJ1c2VyTmFtZSI6ImZsb3dhZ2VudC1zZXJ2aWNlIiwiRElTUExBWV9OQU1FIjoiRmxvd2FnZW50IFNlcnZpY2UgQXBwbGljYXRpb24iLCJMQU5HVUFHRSI6ImVuIn0K"
    }
print("B&R pre testing starting...")
clusterName = sys.argv[1]
shootName = sys.argv[2]
url = sys.argv[3]
tenant = sys.argv[4]
username = sys.argv[5]
password = sys.argv[6]
bIsTestingPassed = True
testStatus = {
    "clusterName": clusterName,
    "shootName": shootName,
    "url": url,
    "tenant": tenant,
    "username": username,
    "password": password,
    "connectionTest":[],
    "dataflowTest":[]
}
def sendRequest(url, headers, payload, method):
    if method == "post":
        res = session.post(url=url,  data=payload, headers=headers, verify=False)
    elif method == "get":
        res = session.get(url)
    return res

def sendPostRequest(url, header, payload):
    res = sendRequest(url, header, payload, "post")
    return res

def sendGetRequest(url):
    res = sendRequest(url,"","", "get")
    return res

def loginDI(baseUrl, tenant, username, password):
    url = baseUrl + "/api/login/v2/finalize"
    password = password.strip("\'")
    payload = {
        "username" : username,
        "password" : password,
        "tenant" : tenant
    }
    sendPostRequest(url, headers, json.dumps(payload))

def getVersion(baseUrl):
    url = baseUrl + "/service/v2/version"
    sendGetRequest(url)

def getConnection(baseUrl, connectionName):
    url = baseUrl + "/app/connection-service/v1/connections/" + connectionName
    connection = sendGetRequest(url)
    print("***----show the connection information-------****")
    print(connection.text)
    return connection.text

def createConnection(baseUrl, connectionName):
    url = baseUrl + "/app/connection-service/v1/connections/"
    fileName = "tests/connection/" + connectionName + ".json"
    f = open(fileName)
    data = json.load(f)
    sendPostRequest(url, headers, json.dumps(data))

def createDataFlow(baseUrl, dataFlowName):
    url = baseUrl + "/app/pipeline-modeler/service/v1/runtime/graphs"
    fileName = "tests/dataflow/" + dataFlowName + ".json"
    f = open(fileName)
    data = json.load(f)
    dataFlowStatus = sendPostRequest(url, headers, json.dumps(data)).text
    dataFlowStatus = json.loads(dataFlowStatus)
    return dataFlowStatus


def getDataFlow(baseUrl, handleID):
    url = baseUrl + "/app/pipeline-modeler/service/v1/runtime/graphs/" + handleID
    dataFlowStatus = sendGetRequest(url).text
    try:
        dataFlowStatus = json.loads(dataFlowStatus)
    except Exception as e:
        print(e.message)
        dataFlowStatus = {
            "handle":"null",
            "status":"null",
            "started":"null"
        }
    return dataFlowStatus

def get_logger():
    # create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(lineno)d - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


# getConnection(url, "GCS_BR_TEST")
# createConnection(url, "gcp_connection")


#time.sleep(5)
#getConnection(url, "GCS_BR_TEST")
# getVersion("https://vsystem.ingress.dis-k6bk7.di-demo.shoot.canary.k8s-hana.ondemand.com")

#createDataFlow(url,"sampleDataflow")

#getDataFlow(url, "9661646b271e414b8ea9cad1364f4540")

def testCreateConnection(connectionName):
    print("****connection******")
    print(connectionName)
    connectionInfo = getConnection(url, connectionName)
    connectionInfo = json.loads(connectionInfo)
    connectionStatus={
        "connectionName": connectionName,
        "isCconnectionExist": False
    }
    try:
        if "statusCode" in connectionInfo and connectionInfo["statusCode"] == 404:
            print("Not find the connection: " + connectionName + ". Creating a new connection.")
            createConnection(url, connectionName)
            time.sleep(5)
            connectionInfo2 = getConnection(url, connectionName)
            connectionInfo2 = json.dumps(connectionInfo2)
            if "statusCode" in connectionInfo2 and connectionInfo2["statusCode"] == 404:
                bIsTestingPassed = False
            else:
                connectionStatus["isCconnectionExist"] = True
        else:
            connectionStatus["isCconnectionExist"] = True
    except Exception as e:
        print(e.message)
        print("Error: Connection creation failed for the connection item: " + connectionName )
    testStatus["connectionTest"].append(connectionStatus)


def testCreateDataFlow(dataflowName):
    dataflowStatus = createDataFlow(url,dataflowName)
    time.sleep(120)
    dataflowDetailsStatus = getDataFlow(url, dataflowStatus["handle"])
    if dataflowDetailsStatus["status"] == "pending":
        print("The graph is still in pending status, wait for more 2 mins")
        time.sleep(120)
        dataflowDetailsStatus = getDataFlow(url, dataflowStatus["handle"])
    if dataflowDetailsStatus["status"] == "stopping":
        print("The graph is still in stopping status, wait for more 1 mins")
        time.sleep(60)
        dataflowDetailsStatus = getDataFlow(url, dataflowStatus["handle"])
    print("Status for the dataflow "+ dataflowName + "is:")
    print(dataflowDetailsStatus)
    if dataflowStatus["handle"] != "null":
        testStatus["dataflowTest"].append({
            "dataflowName": dataflowName,
            "handleId":dataflowStatus["handle"],
            "status": dataflowDetailsStatus["status"],
            "createTime": dataflowDetailsStatus["started"]
        })

logger = get_logger()
print(url)
print(tenant)
print(username)
print(password)
loginDI(url, tenant, username, password)
testCreateConnection("GCS_BR_TEST")
testCreateConnection("CIT_HANA_Cloud")
testCreateConnection("CIT_ADLV2")
testCreateDataFlow("sampleDataflow")
testCreateDataFlow("df_hanaToHana")
testCreateDataFlow("df_adlToHana")


if bIsTestingPassed == False:
    logger.error("##testing for pre-backup is failed")
else:
    logger.info("##testing for pre-backup is all passed")

with open('./tests/result/' + clusterName + "_" + tenant +"_result.json", 'w+') as f:
    f.write(json.dumps(testStatus))
