import os
import ast
import json
import logging
import requests
import time
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

landscape = os.environ.get("LANDSCAPE","testing")
platform = os.environ.get("PLATFORM", "aws")
regional_cluster = "eu"
shoot_name = "dh-z0wbgm6s"
provider = "aws"
if platform == "azure":
    provider = "az"
api_base = "https://staging-controlcenter.datahub.only.sap/api/alpha"
login_timeout=60
proxies = {"http": None, "https": None, }
def requests_retry_session(
        retries=3,
        backoff_factor=1,
        status_forcelist=range(500,600),
        session=None,
    ):

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    request_retry = HTTPAdapter(max_retries=retry)
    session.mount('http://', request_retry)
    session.mount('https://', request_retry)
    return session

def login_cc():
    print("Start to login to the control center.")
    username = os.environ.get('CC_API_USER', 'global\\BDHAUTOENV')
    password = os.environ.get('CC_API_PASSWORD', '@ZYZ)w0DvXf6#ko79')
    #token = os.environ.get('CC_TOKEN', '0f13f457-581a-4cec-bdc1-466584620528')
    auth = requests.auth.HTTPBasicAuth(username,password)
    url = api_base+"/ldap/auth"
    print(url)
    resp = requests_retry_session().post(url, auth=auth, verify=False, proxies=proxies, timeout=login_timeout)
    print(resp.cookies)
    print(resp.status_code)
    print(resp.content)
    token = resp.cookies['session']
    print("token is " + token)
    return token

def get_DHaaS_kubecfg_by_shootname(token, shoot_name):
    url = api_base + "/cluster/admin-kubeconfig?landscape="+landscape+"&provider="+provider+"&shoot="+shoot_name
    cookies = dict(session=token)
    try:
        resp = requests_retry_session().get(url, cookies=cookies, verify=False, timeout=login_timeout)
        if resp.status_code != 200:
            raise Exception('Failed to get DHaaS kubecfg: %s' % resp.content)
        if resp.content.decode() == 'null\n':
            return None
        return resp.content.decode().replace("\\n","\n").strip('\n').strip('\"')
    except Exception as e:
        logging.error('Failed to grab data from CC api, try it later : %s', str(e))
        return None
