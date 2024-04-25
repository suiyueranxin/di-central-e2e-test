import argparse
import json
import os
import uuid

import requests

BROKER_API_VERSION = "2.14"

HELP_MSG_SERVICE_ID = "Service Id"
HELP_MSG_PLAN_ID = "Plan Id"
HELP_MSG_INSTANCE_ID = "Instance Id"
HELP_MSG_BINDING_ID = "Binding Id"

DEFAULT_SERVICE_ID = "740a2273-22be-4fd3-b0e1-1328fd12e98"
DEFAULT_PLAN_ID = "42afee0b-8703-439e-b78b-23fe045c04ff4"

def get_server_info():
    broker_url = os.environ.get('BROKER_URL')
    if not broker_url:
        broker_url = "http://127.0.0.1:8080"

    broker_username = os.environ.get('BROKER_USERNAME')
    if not broker_username:
        broker_username = ""

    broker_password = os.environ.get('BROKER_PASSWORD')
    if not broker_password:
        broker_password = ""

    return broker_url, broker_username, broker_password


def get_catalog(args):
    url, username, password = get_server_info()
    request_url = f'{url}/v2/catalog'
    response = requests.get(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password)
    )
    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def provision_instance(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}'
    params = {
        'accepts_incomplete': 'true'
    }
    print(f'Provisioning instance {args.instance_id}')
    response = requests.put(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params,
        json={
            'service_id': args.service_id,
            'plan_id': args.plan_id,
            'context': {
                'platform': 'local-test',
                'landscape': args.landscape,
                'clusterID': args.cluster
            },
            'parameters': json.loads(args.parameters)
        }
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def update_instance(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}'
    params = {
        'accepts_incomplete': 'true'
    }
    print(f'Updating instance {args.instance_id}')
    response = requests.patch(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params,
        json={
            'service_id': args.service_id,
            'plan_id': args.plan_id,
            'context': {
                'platform': 'local-test',
                'landscape': args.landscape
            },
            'parameters': json.loads(args.parameters)
        }
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def deprovision_instance(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}'
    params = {
        'service_id': args.service_id,
        'plan_id': args.plan_id,
        'accepts_incomplete': 'true'
    }
    print(f'Deprovisioning instance {args.instance_id}')
    response = requests.delete(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def fetch_instance(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}'
    params = {
        'service_id': args.service_id,
        'plan_id': args.plan_id
    }
    print(f'Fetching instance {args.instance_id}')
    response = requests.get(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def get_last_operation(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}/last_operation'
    params = {
        'service_id': args.service_id,
        'plan_id': args.plan_id,
        'operation': args.operation
    }
    print(f'Fetching last operation status for instance {args.instance_id}')
    response = requests.get(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def bind(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}/service_bindings/{args.binding_id}'
    params = {
        'accepts_incomplete': 'true'
    }
    print(f'Rotate credentials for instance {args.instance_id}')
    response = requests.put(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params,
        json={
            'service_id': args.service_id,
            'plan_id': args.plan_id,
            'context': {
                'platform': 'local-test',
            },
            'parameters': {}
        }
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def get_instance_binding(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}/service_bindings/{args.binding_id}'
    params = {
        'service_id': args.service_id,
        'plan_id': args.plan_id
    }
    print(f'Fetching credentials for instance {args.instance_id}')
    response = requests.get(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def get_last_binding_operation(args):
    url, username, password = get_server_info()
    validate(args)
    request_url = f'{url}/v2/service_instances/{args.instance_id}/service_bindings/{args.binding_id}/last_operation'
    params = {
        'service_id': args.service_id,
        'plan_id': args.plan_id,
        'operation': args.operation
    }
    print(f'Fetching last binding operation status for instance {args.instance_id}')
    response = requests.get(
        headers={
            'X-Broker-API-Version': BROKER_API_VERSION
        },
        url=request_url,
        auth=(username, password),
        params=params
    )

    response.raise_for_status()
    print(json.dumps(json.loads(response.text), indent=4))


def validate(args):
    if not args.service_id:
        raise Exception('Can not get service id from parameter -s')

    if not args.plan_id:
        raise Exception('Can not get service id from parameter -p')

    if not args.instance_id:
        raise Exception('Can not get service id from parameter -i')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Service Broker Helper',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers()

    catalog = subparsers.add_parser("catalog", help="Get service catalog")
    catalog.set_defaults(func=get_catalog)

    provision = subparsers.add_parser("provision", help="Provision a service instance")
    provision.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    provision.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    provision.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, default=str(uuid.uuid4()))
    provision.add_argument('-c', dest="parameters", type=str, help="Parameters (json format string)",
                            default='{"tenantUaaUrl": "https://orcastarkiller.com"}')
    provision.add_argument('-l', dest="landscape", type=str, help="Landscape", default="")
    provision.add_argument('-r', dest="cluster", type=str, help="Cluster ID", default="")
    provision.set_defaults(func=provision_instance)

    updating = subparsers.add_parser("update", help="Update a service instance")
    updating.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    updating.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    updating.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, required=True)
    updating.add_argument('-c', dest="parameters", type=str, help="Parameters (json format string)",
                            default='{"tenantUaaUrl": "https://orcastarkiller.com"}')
    updating.add_argument('-l', dest="landscape", type=str, help="Landscape", default="")
    updating.set_defaults(func=update_instance)

    fetching = subparsers.add_parser("fetch", help="Fetch a service instance")
    fetching.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    fetching.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    fetching.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, required=True)
    fetching.set_defaults(func=fetch_instance)

    deprovision = subparsers.add_parser("deprovision", help="Deprovision a service instance")
    deprovision.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    deprovision.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    deprovision.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, required=True)
    deprovision.set_defaults(func=deprovision_instance)

    last_operation = subparsers.add_parser("last-operation", help="Get the operation status")
    last_operation.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    last_operation.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    last_operation.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, required=True)
    last_operation.add_argument('-o', dest='operation', help='Operation', type=str,
                            default='provision')
    last_operation.set_defaults(func=get_last_operation)

    binding = subparsers.add_parser("bind", help="Rotate tenant user credentials")
    binding.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    binding.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    binding.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, required=True)
    binding.add_argument('-b', dest='binding_id', help=HELP_MSG_BINDING_ID, type=str,
                            default='dis-user')
    binding.set_defaults(func=bind)

    get_binding = subparsers.add_parser("get-binding", help="Get tenant user credentials")
    get_binding.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    get_binding.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    get_binding.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, required=True)
    get_binding.add_argument('-b', dest='binding_id', help=HELP_MSG_BINDING_ID, type=str, default='dis-user')
    get_binding.set_defaults(func=get_instance_binding)

    last_binding_operation = subparsers.add_parser("last-binding-operation", help="Get binding operation status")
    last_binding_operation.add_argument('-s', dest="service_id", type=str, help=HELP_MSG_SERVICE_ID, default=DEFAULT_SERVICE_ID)
    last_binding_operation.add_argument('-p', dest="plan_id", type=str, help=HELP_MSG_PLAN_ID, default=DEFAULT_PLAN_ID)
    last_binding_operation.add_argument('-i', dest="instance_id", help=HELP_MSG_INSTANCE_ID, type=str, required=True)
    last_binding_operation.add_argument('-b', dest='binding_id', help=HELP_MSG_BINDING_ID, type=str, default='dis-user')
    last_binding_operation.add_argument('-o', dest='operation', help='Operation', type=str, default='rotate')
    last_binding_operation.set_defaults(func=get_last_binding_operation)

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
