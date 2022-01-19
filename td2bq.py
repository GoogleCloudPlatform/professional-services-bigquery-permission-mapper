# !/usr/bin/env python3

# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The module validates TD permissions and generates JSON and Terraform.

This is the main ACL Mapper module. The user can invoke it to perform
the following:
  Export permissions from Teradata.
  Download the latest td2bq map from a GCS bucket.
  Validate Teradata permissions and create change files.
  Generate JSON files for gcloud CLI to bind BigQuery permissions.
  Generate Terraform scripts to bind BigQuery permissions.
"""
import argparse
import csv
import errno
import json
import logging
import os
import shutil
import sys
import traceback
import uuid
from logging import handlers

import pandas as pd
import teradatasql

from td2bq_mapper import (
    td2bq_arc_map,
    td2bq_create_json,
    td2bq_mod3_map,
    td2bq_perm,
    td2bq_users_map,
    td2bq_util,
)

TD_PERMISSIONS_CSV = "/data/report_permissions.csv"
TD_USERS_AND_ROLES_CSV = "/data/report_users_and_roles.csv"

# file name to report unmapped permissions identified during validation step
INVALID_ACLS_FILE = "unmapped_acls.csv"

LOG_FILE_NAME = "logs/td2bq.log"
LOG_SEPARATOR = "-" * 100
logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger(name="td2bq")


def get_logger(log_path: str):
    """Configure the logger.

    Args:
      log_path: path to the log file
    """
    try:
        if not os.path.exists(os.path.dirname(log_path)):
            os.makedirs(os.path.dirname(log_path))
    except Exception:
        logger.info("Issue while creating log directory")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.error(
            " ".join(
                map(str, traceback.format_exception(exc_type, exc_value, exc_traceback))
            )
        )
    file_handler = handlers.TimedRotatingFileHandler(
        log_path, when="midnight", interval=1, backupCount=30
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def get_arc_permission_map(
    bucket_name: str = None,
    object_name: str = None,
    download_map_file: str = None,
    load_map_file: str = None,
):
    """Download the td2bq map from GCS.

    Args:
      bucket_name: bucket name containing the map file
      object_name: map file name in the bucket
      download_map_file: file name where to download map file
      load_map_file: path to map file
    """
    td2bq = td2bq_arc_map.Td2BqArcMap()
    if bucket_name:
        td2bq.bucket_name = bucket_name
    if object_name:
        td2bq.object_name = object_name

    # save to map file is requested
    if download_map_file:
        my_map = td2bq.get_map_file_from_gcs(download_map_file)
        if my_map is None:
            return

    # use map file requested
    if load_map_file:
        my_map = td2bq.get_td2bq_arc_map(load_map_file)
    else:
        my_map = td2bq.get_td2bq_arc_map()
    # print full map to STDOUT
    if my_map:
        td2bq.print_map(my_map)


def load_perm_from_db(users: str, roles: str, config: str):
    """Exporot Teradata permissions from the database.

    Args:
      users: list of users to export from the database
      roles: list of roles to export from the database
      config: config file describing how to connect to the database
    """
    perm = td2bq_perm.Td2BqPerm(users, roles)

    with open(config) as f:
        connect_str = json.load(f)

    with teradatasql.connect(json.dumps(connect_str)) as connect:
        df = pd.read_sql(perm.get_query_users(), connect)
        df.to_csv(td2bq_util.get_root_dir() + TD_USERS_AND_ROLES_CSV, index=False)
        df = pd.read_sql(perm.get_query_perms(), connect)
        df.to_csv(td2bq_util.get_root_dir() + TD_PERMISSIONS_CSV, index=False)


def create_change_files_from_db(
    users: str = None, roles: str = None, config: str = None, change_folder: str = None
):
    """Export and validate TD permissions a database and output change files.

    Args:
      users: list of users to export from the database
      roles: list of roles to export from the database
      config: cnofig file describing how to connect to the database
      change_folder: path to output change files
    """
    if config is not None:
        load_perm_from_db(users, roles, config)
        create_change_files(change_folder)
        print("Successfully exported user permissions.")
    else:
        raise ValueError(
            f"Provide a database connection file in --config. "
            f"Alternatively, run a query to export permissions: "
            f"{td2bq_perm.Td2BqPerm(users, roles).get_query_perms()}"
        )


def mock_output_file(file: str, suffix: str = "") -> None:
    """Mock user input in change files.

    Mock the change files as if the user names BigQuery objects in the same way
    as in Teradata.

    Args:
        file(str): path to a temp file
        suffix(str): optional suffix added to the mocked column
    """
    tmpfile = file + ".tmp"
    with open(file, "r") as f_in, open(tmpfile, "w") as f_out:
        header = f_in.readline()
        f_out.write(header)
        # Copy second to last column to the last column:
        for line in f_in:
            lline = line.split(",")
            lline.insert(-1, lline[0] + suffix)
            lline = ",".join(lline[:-1])
            f_out.write(lline + "\n")
    os.rename(tmpfile, file)


def validate_acl(td_acl_file, parent_change_folder) -> dict:
    """Validate TD permissions.

    It uses the same validation approach as JSON and Terraform generation steps
    but creates temporary change files and mocks user input in those files.
    Then it analyzes the temp files and removes temp files at the end.

    Args:
        td_acl_file(str): path to TD permissions file
        parent_change_folder(str): directory to create change files specified by
          the user

    Returns:
        dict: dictionary of roles, users, and TD objects, that have Teradata ACL
        that cannot be mapped to BigQuery IAM, i.e n/a permissions
    """
    results = {"roles": set(), "users": set(), "objects": set()}

    # create temp folder for change files
    overwrite = True
    change_folder = os.path.join(parent_change_folder, "tmp" + str(uuid.uuid4()))
    logger.info("Create temp directory %s to validate permissions", change_folder)
    td2bq_util.make_dirs(change_folder, overwrite)

    # create temp change files
    perm = td2bq_perm.Td2BqPerm(td_acl_file)
    perm.create_td2bq_name_change_files_csv(change_folder)
    perm.create_td_role_to_data_to_arc()
    perm.create_user_groups_csv(change_folder)
    logger.info("Created temporary change files in %s", change_folder)

    # mock user changes in temp change files
    for file in [td2bq_perm.CHANGE_FILE_DATASET, td2bq_perm.CHANGE_FILE_OBJECT]:
        mock_output_file(os.path.join(change_folder, file))
    for file in [td2bq_perm.CHANGE_FILE_GROUP, td2bq_perm.CHANGE_FILE_USER]:
        mock_output_file(os.path.join(change_folder, file), "@inc.com")
    logger.info("Updates temporary change files with mock data in %s", change_folder)

    # map permissions in the same way as for JAON/Terraform generation step
    td2bq_mod3 = td2bq_mod3_map.Td2BqMod3Map(overwrite)
    td2bq_arc = td2bq_arc_map.Td2BqArcMap()
    td2bq_permissions = td2bq_perm.Td2BqPerm(td_acl_file)
    arc_file = td2bq_arc.local_map_file
    perm_file = td2bq_permissions.internal_perm_file
    td2bq_mod3.get_bq_role_to_mappings(change_folder, perm_file, arc_file)

    # there should be no errors in the moocked change files
    file = os.path.join(change_folder, td2bq_mod3_map.INVALID_REPORTS_FILE)
    if os.path.isfile(file):
        raise ValueError(
            f"Bug in the ACL Mapper. Please raise a ticket with the "
            f"ACL mapper support: change file error log was generated "
            f"in temp directory ({file})"
        )

    logger.info("Temporary change files were mapped to BibQuery.")
    # Persist reported n/a errors in a separate file and
    # use them to generate change files:
    old_file = os.path.join(change_folder, td2bq_mod3_map.INVALID_ACLS_FILE)
    if os.path.isfile(old_file):
        file = os.path.join(parent_change_folder, INVALID_ACLS_FILE)
        os.rename(old_file, file)
        logger.info(
            "The mapper logged Teradata permissions that cannot be "
            "translated to BigQuery IAM into validation error file: %s",
            file,
        )
        with open(file, "r") as f_invalid:
            csv_reader = csv.DictReader(f_invalid)
            for line in csv_reader:
                results["roles"].add(line["RoleName"])
                results["users"].add(line["UserName"])
                results["objects"].add(line["DataName"])
        results["roles"] = set([value for value in results["roles"] if value != ""])
        results["users"] = set([value for value in results["users"] if value != ""])
        results["objects"] = set(
            [
                "".join(c for c in value if c not in "[]{}\"'")
                for value in results["objects"]
                if value != ""
            ]
        )
    try:
        shutil.rmtree(change_folder, ignore_errors=False)
        logger.info("Temp directory %s was removed", change_folder)
    except Exception:
        logger.warning(
            "Validation finished but the temporary directory %s was "
            "not removed. Please ignore it or remove it manually.",
            change_folder,
        )
    return results


def create_change_files(td_acl_file: str, change_folder: str, overwrite: bool):
    """Validate TD permissions and output change files.

    Validate if all TD permissions can be transalted to BQ IAM.
    It outputs change files, i.e files with TD entities and corresponding
    GCP entities. The user should modify change files to let the ACL mapper know
    what TD entities to map to GCP when generating JSON or Terraform.

    Args:
      td_acl_file(str): path to TD permissions file
      change_folder(str): directory to create change files
      overwrite(bool): If True the Mapper will overwrite the output dirs and files
    """
    # check if we can overwrite the already esicting files
    if not td2bq_util.make_dirs(
        change_folder,
        overwrite,
        {
            td2bq_perm.CHANGE_FILE_GROUP,
            td2bq_perm.CHANGE_FILE_OBJECT,
            td2bq_perm.CHANGE_FILE_DATASET,
            td2bq_perm.CHANGE_FILE_USER,
            INVALID_ACLS_FILE,
        },
    ):
        raise ValueError(f"Could not create the output directory {change_folder}")
    else:
        # Check n/a ACL
        invalid_acl = validate_acl(td_acl_file, change_folder)
        # Output change files only for valid ACL
        perm = td2bq_perm.Td2BqPerm(td_acl_file)
        perm.create_td2bq_name_change_files_csv(change_folder, invalid_acl)
        perm.create_td_role_to_data_to_arc(invalid_acl)
        perm.create_user_groups_csv(change_folder)


def generate_jsons(
    td_acl_file: str, change_folder: str, project_id: str, overwrite: bool
):
    """Generate JSON files for gcloud CLI to assign IAM.

    Based on the user input in change files, the procedure generates JSON files
    containing new BigQuery custom roles and IAM bindings for BigQuery objects.

    Args:
      td_acl_file: path to teradata permissions CSV file
      change_folder: directory to create change files
      project_id: GCP project ID. It is used in generated JSON files
      overwrite: If True the Mapper will overwrite the output dirs and files
    """
    td2bq_mod3 = td2bq_mod3_map.Td2BqMod3Map(overwrite)
    td2bq_arc = td2bq_arc_map.Td2BqArcMap()
    perm = td2bq_perm.Td2BqPerm(td_acl_file)
    td2bq_json = td2bq_create_json.Td2BqJson(change_folder)
    if not (
        td2bq_util.make_dirs(
            td2bq_json.json_folder,
            overwrite,
            {
                td2bq_json.custom_roles_folder,
                td2bq_json.datasets_folder,
                td2bq_json.tables_folder,
            },
        )
        and td2bq_util.make_dirs(td2bq_json.custom_roles_folder, overwrite)
        and td2bq_util.make_dirs(td2bq_json.datasets_folder, overwrite)
        and td2bq_util.make_dirs(td2bq_json.tables_folder, overwrite)
    ):
        raise ValueError(
            f"Could not create all output directories in "
            f"{td2bq_json.json_folder}. Check the log file "
            f"{logger.handlers[0].baseFilename} for details"
        )

    arc_file = td2bq_arc.local_map_file
    perm_file = perm.internal_perm_file
    map_file = td2bq_mod3.get_bq_role_to_mappings(change_folder, perm_file, arc_file)
    dedupe_map = td2bq_mod3.get_dedupe_roles()

    if not td2bq_json.create_json_iam_custom_roles(dedupe_map):
        logger.error(
            "Some custom roles JSON files were not created. Check log "
            "file %s for details.",
            logger.handlers[0].baseFilename,
        )
        return None

    # Code below is deprecated.
    # if not td2bq_json.create_policy_bindings(map_file, dedupe_map, project_id):
    #   logger.error("Some JSON bindings between custom roles and members were "
    #                "not created. Check log file %s for details.",
    #                logger.handlers[0].baseFilename)
    #   return None

    if not td2bq_json.create_dataset_role_json_file(map_file, dedupe_map, project_id):
        logger.error(
            "Some JSON bindings for datasets/tables were not created. "
            "Check log file %s for details.",
            logger.handlers[0].baseFilename,
        )
        return None


def generate_tfscripts(
    td_acl_file: str,
    change_folder: str,
    customer_id: str,
    project_id: str,
    overwrite: bool,
):
    """Generate Terraform scripts to assign IAM.

    Based on the user input in change files, the procedure generates Terraform
    scripts containing new BigQuery custom roles and IAM bindings for BigQuery
    objects.

    Args:
      td_acl_file: path to teradata permissions CSV file
      change_folder: directory containing user edited change files
      customer_id: Google Workspace or Cloud Identity customer ID
      project_id: GCP project ID. It is used in generated Terraform scripts
      overwrite: If True the Mapper will overwrite the output dirs and files
    """
    td2bq_mod3 = td2bq_mod3_map.Td2BqMod3Map()
    td2bq_arc = td2bq_arc_map.Td2BqArcMap()
    perm = td2bq_perm.Td2BqPerm()
    td2bq_users = td2bq_users_map.Td2BqUsersMap()

    arc_file = td2bq_arc.local_map_file
    perm_file = perm.internal_perm_file
    map_file = td2bq_mod3.get_bq_role_to_mappings(change_folder, perm_file, arc_file)
    dedupe_map = td2bq_mod3.get_dedupe_roles()

    if map_file is None:
        logger.error("Error loading in mod3 internal map file")
        return None

    if not td2bq_users.gen_tf_iam_groups(map_file, customer_id, project_id):
        logger.error("Error generating Terraform for Google groups")
        return None

    if not td2bq_users.gen_tf_iam_custom_roles(map_file, dedupe_map, project_id):
        logger.error("Error generating Terraform for IAM custom roles")
        return None

    if not td2bq_users.gen_tf_bq_table_dataset_iam_binding(
        map_file, dedupe_map, project_id
    ):
        logger.error(
            "Error generating Terraform IAM custom roles to BigQuery "
            "datasets bindings"
        )
        return None


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description=__doc__)
        subparsers = parser.add_subparsers(
            dest="content", help="Teradata to BigQuery Permissions Mapping Tool"
        )
        subparsers.required = True

        # Parameters for validation module:
        parser_validate = subparsers.add_parser(
            "validate",
            help="Generate intermediary CSV files containing TD2BQ resources "
            "to be migrated and validated by user.",
        )
        parser_validate.add_argument(
            "--td_acl_file",
            help="Path to the CSV file that contains Teradata permissions",
            required=True,
            type=str,
        )
        parser_validate.add_argument(
            "--change_folder",
            help="The full path to the folder which will contain CSV files with "
            "all GCP resources to be migrated- these files will need "
            "to be modified",
            required=True,
            type=str,
        )
        parser_validate.add_argument(
            "--log",
            dest="log",
            default=os.path.join(
                os.path.abspath(os.path.join(td2bq_util.get_root_dir(), os.pardir)),
                LOG_FILE_NAME,
            ),
            help="Full path to the output log file. Default log file {}".format(
                os.path.join(
                    os.path.abspath(os.path.join(td2bq_util.get_root_dir(), os.pardir)),
                    LOG_FILE_NAME,
                )
            ),
        )
        parser_validate.add_argument(
            "-o",
            "--overwrite",
            dest="overwrite",
            action="store_true",
            help="Overwrite the existing output files",
        )

        # Parameters for Terraform module:
        # NOTE: THIS TERRAFORM GENERATION COMMAND WILL BE ENABLED IN THE NEXT FEW WEEKS
        # parser_getmap = subparsers.add_parser(
        #     "generate_tf", help="Generate BigQuery permissions Terraform scripts."
        # )
        # parser_getmap.add_argument(
        #     "--change_folder",
        #     help="The full path to the folder which will contain modified CSV "
        #     "files with all GCP resources to be migrated",
        #     required=True,
        #     type=str,
        # )
        # parser_getmap.add_argument(
        #     "--customer_ID",
        #     help="Customer ID needed to generate TF script",
        #     required=True,
        #     type=str,
        # )
        # parser_getmap.add_argument(
        #     "--project_ID",
        #     help="GCP project ID of the permissions to be mapped",
        #     required=True,
        #     type=str,
        # )

        # parameters for JSON module
        parser_getjson = subparsers.add_parser(
            "generate_json", help="Generate BigQuery permissions JSON files."
        )
        parser_getjson.add_argument(
            "--td_acl_file",
            help="Path to the CSV file that contains Teradata permissions",
            required=True,
            type=str,
        )
        parser_getjson.add_argument(
            "--change_folder",
            help="The full path to the folder which will contain modified CSV "
            "files with all GCP resources to be migrated",
            required=True,
            type=str,
        )
        parser_getjson.add_argument(
            "--project_ID",
            help="GCP project ID of the permissions to be mapped",
            required=True,
            type=str,
        )
        parser_getjson.add_argument(
            "--log",
            dest="log",
            default=os.path.join(
                os.path.abspath(os.path.join(td2bq_util.get_root_dir(), os.pardir)),
                LOG_FILE_NAME,
            ),
            help="Full path to the output log file. Default log file {}".format(
                os.path.join(
                    os.path.abspath(os.path.join(td2bq_util.get_root_dir(), os.pardir)),
                    LOG_FILE_NAME,
                )
            ),
        )
        parser_getjson.add_argument(
            "-o",
            "--overwrite",
            dest="overwrite",
            action="store_true",
            help="Overwrite the existing output files",
        )
        args = parser.parse_args()

        get_logger(args.log)
        logger.info(LOG_SEPARATOR)
        logger.info("Created an instance of Teradata ACL mapper")
        logger.info(f"Input arguments: {args}")

        if not args:
            print("Please specify a command. See -h option")
        else:
            td_acl_file = td2bq_util.get_abs_path(args.td_acl_file)
            if not os.path.isfile(td_acl_file):
                logger.error(
                    "Teradata permissions file %s does not exist.", td_acl_file
                )
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), td_acl_file
                )
            else:
                change_folder = td2bq_util.get_abs_path(args.change_folder)
                if args.content == "validate":
                    create_change_files(td_acl_file, change_folder, args.overwrite)
                elif args.content == "generate_tf":
                    generate_tfscripts(change_folder, args.customer_ID, args.project_ID)
                elif args.content == "generate_json":
                    generate_jsons(
                        td_acl_file, change_folder, args.project_ID, args.overwrite
                    )
                # The below part is commented out since we plan to opensource it in
                # step 2. We will refactor the below code based on the initial
                # user feedback gathered in step 1.
                # elif args.content == "arcmap":
                #   get_arc_permission_map(bucket_name=args.bucket_name,
                #                          object_name=args.gcs_object_name,
                #                          download_map_file=args.download_map_file,
                #                          load_map_file=args.load_map_file)
                # elif args.content == 'perm':
                #   create_change_files_from_db(users=args.user,
                #                               roles=args.role,
                #                               config=args.config,
                #                               change_folder=args.change_folder)
                logger.info("ACL mapper finished successfully")
    except Exception:
        logger.exception("Fatal exception in ACL Mapper")
        raise
