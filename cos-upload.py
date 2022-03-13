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


def multi_part_upload(bucket_name, item_name, file_path):
    try:
        logging.info("Starting file transfer for {0} to bucket: {1}".format(item_name, bucket_name))
        # set 5 MB chunks
        part_size = 1024 * 1024 * 5

        # set threadhold to 15 MB
        file_threshold = 1024 * 1024 * 15

        # set the transfer threshold and chunk size
        transfer_config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold=file_threshold,
            multipart_chunksize=part_size
        )

        # the upload_fileobj method will automatically execute a multi-part upload
        # in 5 MB chunks for all files over 15 MB
        with open(file_path, "rb") as file_data:
            cos.Object(bucket_name, item_name).upload_fileobj(
                Fileobj=file_data,
                Config=transfer_config
            )
        logging.info("Transfer for {0} complete".format(item_name))
    except ClientError as be:
        logging.error("CLIENT ERROR: {0}".format(be))
    except Exception as e:
        logging.error("Unable to complete multi-part upload: {0}".format(e))


if __name__ == "__main__":
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Upload a file to COS bucket.")
    parser.add_argument('--inifile', '-i', action='store', dest='inifile', help='inifile which contains the required parameters')
    parser.add_argument('--apikey', '-k', default=os.environ.get('cos_apikey', None), action='store', dest='cos_apikey', help='COS apikey')
    parser.add_argument('--bucket', '-b', action='store', dest='cos_bucket', help="Bucket to upload to.")
    parser.add_argument('--file', '-f', action='store', help='file to upload to COS')

    args = parser.parse_args()

    get_config(args)

    # upload created file to COS if COS credentials provide

    cos = ibm_boto3.resource("s3",
                             ibm_api_key_id=args.cos_apikey,
                             ibm_service_instance_id=args.cos_instance_crn,
                             config=Config(signature_version="oauth"),
                             endpoint_url=args.cos_endpoint
                             )

    multi_part_upload(args.cos_bucket, os.path.split(args.file)[1], args.file)

