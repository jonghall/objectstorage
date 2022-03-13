#!/usr/bin/env python3
# Author: Jon Hall
# Copyright (c) 2022
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the Licene is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__author__ = 'jonhall'
import os, configparser, json, os.path, argparse, sys, logging, grpc
sys.path.insert(0, 'aspera-sdk/connector')
import transfer_pb2 as transfer_manager
import transfer_pb2_grpc  as transfer_manager_grpc


def get_config(args):
    if not args.inifile:
        dirpath = os.path.dirname(os.path.realpath(sys.argv[0]))
        config = configparser.ConfigParser()
        ini_file = 'ibmcloud.ini'
        try:
            # attempt to open ini file first. Only proceed if found
            # assume execution from the ansible playbook directory
            filepath = dirpath + '/' + ini_file
            open(filepath)

        except FileNotFoundError:
            raise Exception("Unable to find or open specified ini file")
        else:
            config.read(filepath)

        config.read(filepath)

        if 'cos_endpoint' in config["ibmcloud"]:
            args.cos_endpoint= config["ibmcloud"]["cos_endpoint"]
        else:
            print ("You must specify a COS Endpoint")
            quit()

        if 'cos_instance_crn' in config["ibmcloud"]:
            args.cos_instance_crn= config["ibmcloud"]["cos_instance_crn"]
        else:
            print ("You must specify a COS Instance CRN")
            quit()

        if 'cos_bucket' in config["ibmcloud"] and args.cos_bucket==None:
            args.cos_bucket= config["ibmcloud"]["cos_bucket"]

    return args

def upload(args):
    # create a connection to the transfer manager daemon
    client = transfer_manager_grpc.TransferServiceStub(
        grpc.insecure_channel('localhost:55002'))

    # create transfer spec
    transfer_spec = {
        "session_initiation": {
            "icos": {
                "api_key": args.cos_apikey,
                "bucket": args.cos_bucket,
                "ibm_service_instance_id": args.cos_instance_crn,
                "ibm_service_endpoint": args.cos_endpoint
            }
        },
        "direction": "recv",
        "remote_host": "https://ats-sl-fra.aspera.io:443",
        "title": "strategic",
        "assets": {
            "destination_root": os.getcwd(),
            "paths": [
                {
                    "source": args.file
                }
            ]
        }
    }
    transfer_spec = json.dumps(transfer_spec)

    # create a transfer request
    transfer_request = transfer_manager.TransferRequest(
        transferType=transfer_manager.FILE_REGULAR,
        config=transfer_manager.TransferConfig(),
        transferSpec=transfer_spec)

    # send start transfer request to transfer manager daemon
    transfer_response = client.StartTransfer(transfer_request)
    transfer_id = transfer_response.transferId
    print("transfer started with id {0}".format(transfer_id))

    # monitor transfer status
    for transfer_info in client.MonitorTransfers(
            transfer_manager.RegistrationRequest(
                filters=[transfer_manager.RegistrationFilter(
                    transferId=[transfer_id]
                )])):
        #print("transfer info {0}".format(transfer_info))

        # check transfer status in response, and exit if it's done
        status = transfer_info.status
        if status == transfer_manager.FAILED or status == transfer_manager.COMPLETED:
            print("finished {0}".format(status))
            break

if __name__ == "__main__":
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Download a file from COS bucket using Aspera.")
    parser.add_argument('--inifile', '-i', action='store', dest='inifile', help='inifile which contains the required parameters')
    parser.add_argument('--apikey', '-k', default=os.environ.get('cos_apikey', None), action='store', dest='cos_apikey', help='COS apikey')
    parser.add_argument('--bucket', '-b', action='store', dest='cos_bucket', help="Bucket to download from.")
    parser.add_argument('--file', '-f', action='store', help='file to download from COS')

    args = parser.parse_args()

    get_config(args)

    upload(args)

