#!/usr/bin/env python3
# Author: Jon Hall
# Copyright (c) 2022
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__author__ = 'jonhall'
import os, configparser, json, os.path, argparse, sys, logging
import ibm_boto3
from ibm_botocore.client import Config, ClientError

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

if __name__ == "__main__":
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Download a file from a COS bucket.")
    parser.add_argument('--inifile', '-i', action='store', dest='inifile', help='inifile which contains the required parameters')
    parser.add_argument('--apikey', '-k', default=os.environ.get('cos_apikey', None), action='store', dest='cos_apikey', help='COS apikey')
    parser.add_argument('--bucket', '-b', action='store', dest='cos_bucket', help="Bucket to download from.")
    parser.add_argument('--file', '-f', action='store', help='file to download from COS')

    args = parser.parse_args()

    get_config(args)

    # upload created file to COS if COS credentials provided
    cos = ibm_boto3.resource("s3",
                             ibm_api_key_id=args.cos_apikey,
                             ibm_service_instance_id=args.cos_instance_crn,
                             config=Config(signature_version="oauth"),
                             endpoint_url=args.cos_endpoint
                             )
    logging.info("Downloading file {0} from bucket {1}.".format(args.file, args.cos_bucket))
    cos.Object(args.cos_bucket, args.file).download_file(args.file)
    logging.info("Downloading of file {0} complete.".format(args.file))

