import os
import logging
import shutil
import requests
import json
import time
import sys
import unittest
import xmlrunner
import argparse

#unittest.main(argv=['first-arg-is-ignored'],testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
session = requests.Session()
headers = {
        "Content-Type": "application/json",
        'accept': 'application/json',
        'X-Requested-With': 'Fetch',
        "x-sap-boc-user-properties-b64": "eyJ1c2VyTmFtZSI6ImZsb3dhZ2VudC1zZXJ2aWNlIiwiRElTUExBWV9OQU1FIjoiRmxvd2FnZW50IFNlcnZpY2UgQXBwbGljYXRpb24iLCJMQU5HVUFHRSI6ImVuIn0K"
    }

# clusterName = sys.argv[1]
# url = sys.argv[2]
# tenant = sys.argv[3]
# username = sys.argv[4]
# password = sys.argv[5]

clusterName = os.getenv('CLUSTERNAME')
url = os.getenv('URL')
tenant = os.getenv('TENANT')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
bIsValidationPassed = True
resultString = ""

def sendRequest(url, headers, payload, method):
    if method == "post":
        res = session.post(url=url,  data=payload, headers=headers)
    elif method == "get":
        res = session.get(url)
    return res

def sendPostRequest(url, header, payload):
    print("send the post request to URL:" + url)
    res = sendRequest(url, header, payload, "post")
    return res

def sendGetRequest(url):
    print("send the get request to URL:" + url)
    res = sendRequest(url,"","", "get")
    return res

def loginDI(baseUrl, tenant, username, password):
    password = password.strip("\'")
    print("Try to login system")
    url = baseUrl + "/api/login/v2/finalize"
    print("URL: " + url)
    print("Tenant: " + tenant)
    print("Username:" + username)
    print("Password:" + password)
    payload = {
        "username" : username,
        "password" : password,
        "tenant" : tenant
    }
    res = sendPostRequest(url, headers, json.dumps(payload))
    print("****Login information****")
    print(res.text)

def getVersion(baseUrl):
    url = baseUrl + "/service/v2/version"
    sendGetRequest(url)

def getConnection(baseUrl, connectionName):
    url = baseUrl + "/app/connection-service/v1/connections/" + connectionName
    print("Try to get the connection with url:" + url)
    connection = sendGetRequest(url)
    return connection.text

def createDataFlow(baseUrl, dataFlowName):
    url = baseUrl + "/app/pipeline-modeler/service/v1/runtime/graphs"
    fileName = "dataflow" + dataFlowName + ".json"
    f = open(fileName)
    data = json.load(f)
    print("Try to create the graph:" + fileName)
    dataFlowStatus = sendPostRequest(url, headers, json.dumps(data)).text
    dataFlowStatus = json.loads(dataFlowStatus)
    return dataFlowStatus

def getDataFlowsByRestartID(baseUrl, restartID):
    url = baseUrl + "/app/pipeline-modeler/service/v1/runtime/graphsquery"
    query = {"filter":["equal","autoRestartInfo.id",restartID],"detailLevel":"graph"}
    dataFlowsStatus = sendPostRequest(url, headers, json.dumps(data)).text
    if "not found" in dataFlowsStatus:
        dataFlowsStatus = {
            "status": "not found"
        }
        return dataFlowsStatus
    dataFlowsStatus = json.loads(dataFlowsStatus)
    return dataFlowsStatus

def getDataFlow(baseUrl, handleID):
    url = baseUrl + "/app/pipeline-modeler/service/v1/runtime/graphs/" + handleID
    dataFlowStatus = sendGetRequest(url).text
    print("status of " + handleID + " is:")
    print(dataFlowStatus)
    if "not found" in dataFlowStatus:
        dataFlowStatus = {
            "status": "not found"
        }
        return dataFlowStatus
    try:
        dataFlowStatus = json.loads(dataFlowStatus)
    except Exception as e:
        print(e.message)
        dataFlowStatus = {
            "status":"not found"
        }
    
    return dataFlowStatus

def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(lineno)d - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def readPreData():
    fileName = "./tests/result/" + clusterName + "_" + tenant + "_result.json"
    print("pre data's file is: ")
    print(fileName)
    f = open(fileName)
    data = json.load(f)
    return data

def readResultData():
    fileName = "./tests/result/" + clusterName + "_" + tenant +  "_restore_result.json"
    f = open(fileName)
    data = json.load(f)
    return data

def getRestoreConnectionByID(connectionID):
    fullData = readResultData()
    for connection in fullData["connectionTest"]:
        connectionNAME = connection["connectionName"]
        if connectionNAME == connectionID:
            return connection
    connectionNotFound = {
            "connectionName": "Connection_not_found",
            "isCconnectionExist": True,
            "isExistAfterRestore": False
        }
    return connectionNotFound

def getRestoreDFByID(dataflowID):
    fullData = readResultData()
    for dataflow in fullData["dataflowTest"]:
        dataflowName = dataflow["dataflowName"]
        if dataflowName == dataflowID:
            return dataflow
    dataflowNotFound = {
            "dataflowName": "dataflow_not_found",
            "handleId": "dataflow_not_found",
            "status": "dataflow_not_found",
            "createTime": 0,
            "isSameStatusAfterRestore": False,
            "statusAfterRestore":"error"
        }
    return dataflowNotFound

    
print("start post restore testing")
loginDI(url,tenant,username,password)
preData = readPreData()
for connection in preData["connectionTest"]:
    connectionNAME = connection["connectionName"]
    connectionDetails = getConnection(url, connectionNAME)
    connectionDetails = json.loads(connectionDetails)
    if "technicalName" in connectionDetails and connectionDetails["technicalName"] == connectionNAME:
        connection["isExistAfterRestore"] = True
        print("The connection " + connectionNAME + " is exist.")
        resultString = resultString + "\r\n<br> The connection " + connectionNAME + "is pass."
    else:
        connection["isExistAfterRestore"] = False
        print("Error: The connection " + connectionNAME + " is not exist.")
        resultString = resultString + "\r\n<br>  The connection " + connectionNAME + "is failed."

for dataFlow in preData["dataflowTest"]:
    print(url)
    print(dataFlow["handleId"])
    dataFlowDetails = getDataFlow(url, dataFlow["handleId"])
    if "status" in dataFlowDetails and dataFlowDetails["status"] == dataFlow["status"]:
        dataFlow["isSameStatusAfterRestore"] = True
        dataFlow["statusAfterRestore"] = dataFlowDetails["status"]
        resultString = resultString + "\r\n<br>  The dataFlow " + dataFlow["dataflowName"] + "is pass."
    elif "status" in dataFlowDetails and dataFlowDetails["status"] == "dead" and dataFlow["status"] == "running":
        dataFlow["isSameStatusAfterRestore"] = False
        dataFlow["statusAfterRestore"] = dataFlowDetails["status"]
        if "autoRestartInfo" in dataFlowDetails:
            restartDataFlowDetails = getDataFlowsByRestartID(url, dataFlowDetails["autoRestartInfo"]["id"])
            for restartDataFlow in restartDataFlowDetails:
                if restartDataFlow["status"] == "running":
                    resultString = resultString + "\r\n<br>  The dataFlow " + dataFlow["dataflowName"] + "is pass."
                    dataFlow["isSameStatusAfterRestore"] = True
                    dataFlow["statusAfterRestore"] = restartDataFlow["status"]
                else:
                    resultString = resultString + "\r\n<br>  The dataFlow " + dataFlow["dataflowName"] + "is failed."
        else:
            resultString = resultString + "\r\n<br>  The dataFlow " + dataFlow["dataflowName"] + "is failed."
    else:
        dataFlow["isSameStatusAfterRestore"] = False
        resultString = resultString + "\r\n<br>  The dataFlow " + dataFlow["dataflowName"] + "is failed."

with open('./tests/result/' + clusterName + "_" + tenant +  "_restore_result.json", 'w+') as f:
    f.write(json.dumps(preData))
with open('./tests/result/resultForEmail.txt', 'w+') as f:
    f.write(resultString)

class testRestoreConnection(unittest.TestCase):
    def test_GCS_BR_TEST(self):
        connectionObj = getRestoreConnectionByID("GCS_BR_TEST")
        self.assertEqual(connectionObj["isCconnectionExist"],connectionObj["isExistAfterRestore"] )

    def test_CIT_HANA_Cloud(self):
        connectionObj = getRestoreConnectionByID("CIT_HANA_Cloud")
        self.assertEqual(connectionObj["isCconnectionExist"],connectionObj["isExistAfterRestore"] )

    def test_CIT_ADLV2(self):
        connectionObj = getRestoreConnectionByID("CIT_ADLV2")
        self.assertEqual(connectionObj["isCconnectionExist"],connectionObj["isExistAfterRestore"] )

class testRestoreDataflow(unittest.TestCase):
    def test_sampleDataflow(self):
        dataflowObj = getRestoreDFByID("sampleDataflow")
        self.assertEqual(dataflowObj["status"],dataflowObj["statusAfterRestore"] )

    def test_df_hanaToHana(self):
        dataflowObj = getRestoreDFByID("df_hanaToHana")
        self.assertEqual(dataflowObj["status"],dataflowObj["statusAfterRestore"] )

    def test_df_adlToHana(self):
        dataflowObj = getRestoreDFByID("df_adlToHana")
        self.assertEqual(dataflowObj["status"],dataflowObj["statusAfterRestore"] )

if __name__ == '__main__':
    unittest.main()