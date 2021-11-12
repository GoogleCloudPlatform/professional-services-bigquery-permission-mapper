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

import copy
import csv
import json
import logging
import os
import re
import sys
import traceback

import validate_email

from td2bq_mapper import td2bq_perm

# file name to report invalid records in change files provided by the user
INVALID_REPORTS_FILE = "invalid_reports.csv"

# file name to report unmapped permissions identified during JSON/Terraform step
INVALID_ACLS_FILE = "invalid_acls.csv"

# column names in invalid report CSV file
FIELDS_INVALID_REPORTS = [
    "RoleName",
    "UserName",
    "GroupName",
    "AccessRight",
    "DataName",
    "ErrorDescription",
]

# file name for the td2bq mapping report
BQ_MAPPING_CSV = "td2bq_mapping.csv"

# definition for unmapped permissions in the map file
NA = "n/a"

logger = logging.getLogger("td2bq")


class Td2BqMod3Map:
    """Parse user input, instantiate and store bq_role_to_mappings."""

    def __init__(self, overwrite: bool) -> None:
        """Td2BqMod3Map constructor. Initialize class variables.

        Args:
          overwrite(bool): If True the Mapper will overwrite the output dirs
          and files
        """
        self.td2bq_users = None
        self.td2bq_datasets = None
        self.td2bq_objects = None
        self.td2bq_groups = None
        self.td_role_to_users = dict()
        self.td_role_to_data_to_arc = dict()
        self.td_user_to_data_to_arc = dict()
        # Deprecated. Delete in the next version
        self.valid_roles_to_data_to_arc = dict()
        # Deprecated. Delete in the next version
        self.valid_users_to_data_to_arc = dict()
        self.td_role_to_arc_to_data = dict()
        self.td_user_to_arc_to_data = dict()
        self.arc_to_bq_role = dict()  #
        self.bq_role_to_iam = dict()  #
        self.bq_group_to_roles = dict()  #
        self.bq_role_to_mappings = dict()  #
        self.dedupe_mapping = dict()
        self.bq_acl_na = dict()
        self.bq_role_na = set()
        self.overwrite = overwrite

    def create_invalid_report(self, invalid_file: str):
        """Create a file with a standard header.

        Args:
          invalid_file: path to file to create
        """
        if not os.path.exists(invalid_file):
            with open(invalid_file, "w") as f:  # create new invalid file in folder
                writer = csv.DictWriter(f, fieldnames=FIELDS_INVALID_REPORTS)
                writer.writeheader()

    def convert_users_input(self, invalid_file: str, change_folder: str) -> bool:
        """Converts user change file content into a dict.

        Opens the users file inputted by user and converts it to dictionary
        format for use. Dictionary do not include users that will not
        be migrated.

        Args:
          invalid_file(str): full path to the output invalid CSV file
          change_folder(str): path to folder to dump files for the user to change

        Returns:
          bool: True on success.
        """
        td2bq_user_file = os.path.join(change_folder, td2bq_perm.CHANGE_FILE_USER)
        try:
            with open(td2bq_user_file, "r") as user_f:
                reader = csv.reader(user_f)
                next(reader)
                self.td2bq_users = dict()
                for td_user, bq_user in reader:
                    td_user = td_user.strip()
                    bq_user = bq_user.strip()
                    if bq_user != "":
                        if validate_email.validate_email(bq_user):
                            self.td2bq_users[td_user] = bq_user
                        else:
                            self.create_invalid_report(invalid_file)
                            with open(invalid_file, "a", newline="") as invalid_f:
                                writer = csv.DictWriter(
                                    invalid_f, fieldnames=FIELDS_INVALID_REPORTS
                                )
                                writer.writerow(
                                    {
                                        "UserName": td_user,
                                        "ErrorDescription": f"BigQuery user renamed to '{bq_user}' is "
                                        f"not an email",
                                    }
                                )
                            logger.warning(
                                "BigQuery user renamed to %s is not an email", bq_user
                            )
        except Exception:
            logger.exception("Error, Could not load user file: $s", td2bq_user_file)
            raise
        return True

    def convert_datasets_input(self, invalid_file: str, change_folder: str) -> bool:
        """Converts dataset change file content into a dict.

        Opens the dataset file inputted by user and converts it to dictionary
        format for use. Dictionary do not include datasets that will not
        be migrated.

        Args:
          invalid_file(string): full path to the invalid CSV output file.
          change_folder(str): path to folder to dump files for the user to change

        Returns:
            bool: True on success.
        """
        td2bq_dataset_file = os.path.join(change_folder, td2bq_perm.CHANGE_FILE_DATASET)
        try:
            with open(td2bq_dataset_file, "r") as dataset_f:
                reader = csv.reader(dataset_f)
                next(reader)
                self.td2bq_datasets = dict()
                for td_database, bq_dataset in reader:
                    td_database = td_database.strip()
                    bq_dataset = bq_dataset.strip()
                    if bq_dataset != "":
                        if re.match(r"^[\w]+$", bq_dataset):
                            if (
                                bq_dataset not in self.td2bq_datasets.values()
                            ):  # catch duplicate dataset name
                                self.td2bq_datasets[td_database] = bq_dataset
                            else:
                                self.create_invalid_report(invalid_file)
                                with open(invalid_file, "a", newline="") as invalid_f:
                                    writer = csv.DictWriter(
                                        invalid_f, fieldnames=FIELDS_INVALID_REPORTS
                                    )
                                    writer.writerow(
                                        {
                                            "DataName": td_database,
                                            "ErrorDescription": f"BigQuery dataset renamed to '{bq_dataset}' "
                                            f"is not unique",
                                        }
                                    )
                                logger.warning(
                                    "BigQuery dataset renamed to $s is not uniquely "
                                    "named.",
                                    bq_dataset,
                                )
                        else:  # catch invalid dataset naming
                            self.create_invalid_report(invalid_file)
                            with open(invalid_file, "a", newline="") as invalid_f:
                                writer = csv.DictWriter(
                                    invalid_f, fieldnames=FIELDS_INVALID_REPORTS
                                )
                                writer.writerow(
                                    {
                                        "DataName": td_database,
                                        "ErrorDescription": f"BigQuery dataset renamed to '{bq_dataset}' can "
                                        f"only contain letters, numbers, "
                                        f"and underscores",
                                    }
                                )
                            logger.warning(
                                "BigQuery dataset renamed to %s contains "
                                "invalid characters.",
                                bq_dataset,
                            )
        except Exception:
            logger.exception(
                "Error, Could not load datasets file: %s", td2bq_dataset_file
            )
            raise
        return True

    def convert_objects_input(self, invalid_file: str, change_folder: str) -> bool:
        """Converts object change file content into a dict.

        Opens the object file inputted by user and converts it to dictionary
        format for use. Dictionary do not include objects that will not
        be migrated.

        Args:
          invalid_file(string): full path to the invalid CSV output file.
          change_folder(str): path to folder to dump files for the user to change

        Returns:
            bool: True on success.
        """
        td2bq_object_file = os.path.join(change_folder, td2bq_perm.CHANGE_FILE_OBJECT)
        obj_duplicates = {}
        obj_invalids = set()
        try:
            with open(td2bq_object_file, "r") as obj_f:
                reader = csv.reader(obj_f)
                next(reader)
                self.td2bq_objects = dict()
                for td_obj, bq_obj in reader:
                    td_obj = td_obj.strip()
                    bq_obj = bq_obj.strip()
                    if bq_obj != "":
                        only_table = re.search(r"[^.]+$", bq_obj).group(
                            0
                        )  # gets the BQ table name
                        if re.match(r"^[\w]+$", only_table):
                            if only_table not in obj_duplicates:
                                obj_duplicates[only_table] = {
                                    "datasets": [td_obj.split(".")[0]],
                                    "objects": [td_obj],
                                }
                                renamed_bq_dataset = self.td2bq_datasets[
                                    td_obj.split(".")[0]
                                ]
                                # appends new table name to new dataset
                                self.td2bq_objects[td_obj] = (
                                    renamed_bq_dataset + "." + only_table
                                )
                            elif (
                                td_obj.split(".")[0]
                                not in obj_duplicates[only_table]["datasets"]
                            ):  # catch duplicate obj name
                                obj_duplicates[only_table]["datasets"].append(
                                    td_obj.split(".")[0]
                                )
                                obj_duplicates[only_table]["objects"].append(td_obj)
                                renamed_bq_dataset = self.td2bq_datasets[
                                    td_obj.split(".")[0]
                                ]
                                # appends new table name to new dataset
                                self.td2bq_objects[td_obj] = (
                                    renamed_bq_dataset + "." + only_table
                                )
                            else:
                                obj_invalids.add(only_table)
                                obj_duplicates[only_table]["objects"].append(td_obj)
                        else:  # catch invalid dataset naming
                            self.create_invalid_report(invalid_file)
                            with open(invalid_file, "a", newline="") as invalid_f:
                                writer = csv.DictWriter(
                                    invalid_f, fieldnames=FIELDS_INVALID_REPORTS
                                )
                                writer.writerow(
                                    {
                                        "DataName": bq_obj,
                                        "ErrorDescription": f"BigQuery object renamed to '{bq_obj}' can "
                                        f"only contain letters, numbers, "
                                        f"and underscores",
                                    }
                                )
                            logger.warning(
                                "BigQuery object renamed to %s contains invalid "
                                "characters.",
                                bq_obj,
                            )
            if obj_invalids:
                self.create_invalid_report(invalid_file)
                with open(invalid_file, "a", newline="") as invalid_f:
                    writer = csv.DictWriter(
                        invalid_f, fieldnames=FIELDS_INVALID_REPORTS
                    )
                    for bq_obj in obj_invalids:
                        writer.writerow(
                            {
                                "DataName": bq_obj,
                                "ErrorDescription": f"BigQuery object renamed to '{bq_obj}' is matched "
                                f"to multiple Teradata objects: "
                                f"{obj_duplicates[bq_obj]['objects']}",
                            }
                        )
                        logger.warning(
                            "BigQuery object renamed to %s is matched to "
                            "multiple Teradata objects: %s",
                            bq_obj,
                            {obj_duplicates[bq_obj]["objects"]},
                        )
        except Exception:
            logger.exception(
                "Error, Could not load objects file: %s", td2bq_object_file
            )
            raise
        return True

    def convert_groups_input(self, invalid_file: str, change_folder: str) -> bool:
        """Converts group change file content into a dict.

        Opens the group file inputted by user and converts it to dictionary
        format for use. Dictionaries does not include roles that will not
        be migrated.

        Args:
          invalid_file(string): full path to the output invalid CSV file.
          change_folder(str): path to folder to dump files for the user to change

        Returns:
            bool: True on success.
        """
        td2bq_group_file = os.path.join(change_folder, td2bq_perm.CHANGE_FILE_GROUP)
        try:
            with open(td2bq_group_file, "r") as group_f:
                reader = csv.reader(group_f)
                next(reader)
                self.td2bq_groups = dict()
                for td_role, group, renamed_group in reader:
                    td_role = td_role.strip()
                    renamed_group = renamed_group.strip()
                    if renamed_group != "":
                        if validate_email.validate_email(renamed_group):
                            if renamed_group not in self.td2bq_groups:
                                self.td2bq_groups[renamed_group] = set()
                            self.td2bq_groups[renamed_group].add(td_role)
                        else:
                            self.create_invalid_report(invalid_file)
                            with open(invalid_file, "a", newline="") as invalid_f:
                                writer = csv.DictWriter(
                                    invalid_f, fieldnames=FIELDS_INVALID_REPORTS
                                )
                                writer.writerow(
                                    {
                                        "RoleName": td_role,
                                        "GroupName": group,
                                        "ErrorDescription": f"BigQuery group renamed to '{renamed_group}' is "
                                        f"not an email",
                                    }
                                )
                            logger.warning(
                                "BigQuery group renamed to %s is NOT an email",
                                renamed_group,
                            )
        except Exception:
            logger.exception("Error, Could not load groups file: %s", td2bq_group_file)
            raise
        return True

    def create_td_role_to_users(self, perm_file: str) -> bool:
        """Performs internal mapping of TD role to TD users.

        Populates self.td_role_to_users

        Args:
          perm_file(string): full path to perm CSV input file.

        Returns:
          bool: True on success.
        """
        try:
            with open(perm_file, "r") as perms_f:
                reader = csv.DictReader(perms_f)
                for row in reader:
                    td_role = row["RoleName"].strip()
                    td_user = row["UserName"].strip()
                    if td_user in self.td2bq_users:
                        # create mapping for users that user wants to migrate
                        if td_role == "" or td_role in set().union(
                            *self.td2bq_groups.values()
                        ):
                            if td_role not in self.td_role_to_users:
                                self.td_role_to_users[td_role] = set()
                            self.td_role_to_users[td_role].add(td_user)
        except Exception:
            logger.exception("Error, Could not load file: %s", perm_file)
            raise
        return True

    def create_data_to_arc(self, perm_file: str) -> bool:
        """Groups TD users and roles by ARC.

        Internal mappings of TD roles and users to TD datasets/objects to their
        respective ARCs (set).

        Args:
          perm_file(string): full path to perm CSV input file.

        Returns:
          bool: True on success.
        """
        try:
            with open(perm_file, "r") as perms_f:
                reader = csv.DictReader(perms_f)
                for row in reader:
                    td_role = row["RoleName"].strip()
                    td_user = row["UserName"].strip()
                    td_database = row["DatabaseName"].strip()
                    td_object = row["TableName"].strip()
                    td_db_obj = row["DatabaseName"].strip() + "." + row["TableName"]
                    arc = row["AccessRight"].strip()

                    # skip over the rows the user doesn't want to migrate
                    if (
                        td_user in self.td2bq_users
                        and td_database in self.td2bq_datasets
                        and (
                            td_role in set().union(*self.td2bq_groups.values())
                            or td_role == ""
                        )
                    ):
                        if td_object == "All" or td_db_obj in self.td2bq_objects:
                            if td_role == "":  # user not assigned to a role
                                if td_user not in self.td_user_to_data_to_arc:
                                    self.td_user_to_data_to_arc[td_user] = dict()

                                if td_object == "All":
                                    if (
                                        td_database
                                        not in self.td_user_to_data_to_arc[td_user]
                                    ):
                                        self.td_user_to_data_to_arc[td_user][
                                            td_database
                                        ] = set()
                                    self.td_user_to_data_to_arc[td_user][
                                        td_database
                                    ].add(arc)
                                else:
                                    database_table_name = td_database + "." + td_object
                                    if (
                                        database_table_name
                                        not in self.td_user_to_data_to_arc[td_user]
                                    ):
                                        self.td_user_to_data_to_arc[td_user][
                                            database_table_name
                                        ] = set()
                                    self.td_user_to_data_to_arc[td_user][
                                        database_table_name
                                    ].add(arc)
                            else:  # user is assigned to a role
                                if td_role not in self.td_role_to_data_to_arc:
                                    self.td_role_to_data_to_arc[td_role] = dict()

                                if td_object == "All":
                                    if (
                                        td_database
                                        not in self.td_role_to_data_to_arc[td_role]
                                    ):
                                        self.td_role_to_data_to_arc[td_role][
                                            td_database
                                        ] = set()
                                    self.td_role_to_data_to_arc[td_role][
                                        td_database
                                    ].add(arc)
                                else:
                                    database_table_name = td_database + "." + td_object
                                    if (
                                        database_table_name
                                        not in self.td_role_to_data_to_arc[td_role]
                                    ):
                                        self.td_role_to_data_to_arc[td_role][
                                            database_table_name
                                        ] = set()
                                    self.td_role_to_data_to_arc[td_role][
                                        database_table_name
                                    ].add(arc)
        except Exception:
            logger.exception("Error, Could not load file: $s", perm_file)
            raise
        return True

    def catch_invalid(self, invalid_file: str) -> bool:
        """Catches invalid ARCs (Deprecated).

        Catches invalid ARCs associated with TD roles and TD users related to
        RUID permissions.
        Creates mappings only of valid entries.

        Deprecated. It isn't used in the code.
        Plan to remove the method in the next ACL Mapper version.

        Args:
          invalid_file(str): full path to invalid CSV output file.

        Returns:
          bool: True on success.
        """
        self.valid_roles_to_data_to_arc = copy.deepcopy(self.td_role_to_data_to_arc)
        self.valid_users_to_data_to_arc = copy.deepcopy(self.td_user_to_data_to_arc)

        try:
            results = []
            ruid = {"R", "U", "I", "D"}

            for td_role, data_to_arc in self.td_role_to_data_to_arc.items():
                for data, arc in data_to_arc.items():
                    if ("U" in arc or "I" in arc or "D" in arc) and len(
                        ruid.intersection(arc)
                    ) != 4:
                        results.append(
                            {
                                "RoleName": td_role,
                                "AccessRight": str(arc),
                                "DataName": data,
                                "ErrorDescription": "BigQuery can grant (R,I,U,D) or no data "
                                "modification is allowed",
                            }
                        )
                        # remove invalid from mapping
                        self.valid_roles_to_data_to_arc[td_role].pop(data)

            for td_user, data_to_arc in self.td_user_to_data_to_arc.items():
                for data, arc in data_to_arc.items():
                    if ("U" in arc or "I" in arc or "D" in arc) and len(
                        ruid.intersection(arc)
                    ) != 4:
                        results.append(
                            {
                                "UserName": td_user,
                                "AccessRight": str(arc),
                                "DataName": data,
                                "ErrorDescription": "BigQuery can grant (R,I,U,D) or no data "
                                "modification is allowed",
                            }
                        )
                        # remove invalid from mapping
                        self.valid_users_to_data_to_arc[td_user].pop(data)

            if results:
                self.create_invalid_report(invalid_file)
                with open(invalid_file, "a", newline="") as invalid_f:
                    writer = csv.DictWriter(invalid_f, FIELDS_INVALID_REPORTS)
                    writer.writerows(results)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.error(
                " ".join(
                    map(
                        str,
                        traceback.format_exception(exc_type, exc_value, exc_traceback),
                    )
                )
            )
            raise
        return True

    def create_arc_to_data(self) -> bool:
        """Accumulate data that have the same ARC of each TD role.

        Internal mappings to accumulate data that have the same ARC of each TD
        role. Making the ARC keys into sorted strings for easy lookup when used.
        Populates self.td_role_to_arc_to_data and self.td_user_to_arc_to_data

        Returns:
            bool: True on success.
        """
        for td_role, data_to_arc in self.valid_roles_to_data_to_arc.items():
            if td_role not in self.td_role_to_arc_to_data:
                self.td_role_to_arc_to_data[td_role] = dict()
            for data, arc_set in data_to_arc.items():
                arc_abc_str = str(sorted(arc_set))
                if arc_abc_str not in self.td_role_to_arc_to_data[td_role]:
                    self.td_role_to_arc_to_data[td_role][arc_abc_str] = set()
                self.td_role_to_arc_to_data[td_role][arc_abc_str].add(data)

        for td_user, data_to_arc in self.valid_users_to_data_to_arc.items():
            if td_user not in self.td_user_to_arc_to_data:
                self.td_user_to_arc_to_data[td_user] = dict()
            for data, arc_set in data_to_arc.items():
                arc_abc_str = str(sorted(arc_set))
                if arc_abc_str not in self.td_user_to_arc_to_data[td_user]:
                    self.td_user_to_arc_to_data[td_user][arc_abc_str] = set()
                self.td_user_to_arc_to_data[td_user][arc_abc_str].add(data)

        return True

    def _get_composite_acl(self, arc_map: dict) -> list:
        """Returns composite ACLs from the map.

        Returns composite ACLs from the map file. E.g R,U,I,D is a composite ACL

        Args:
          arc_map(dict): ARC JSON.

        Returns:
          list: a list of composite ACLs.
        """
        result = [arc for arc in arc_map.keys() if "," in arc]
        return result

    def _loop_through_mapping(
        self,
        valid_roles_to_data_to_arc,
        bq_role_count: int,
        arc_map: dict,
        arc_composite: dict,
    ) -> int:
        """Map Teradata roles to BigQuery roles and IAM.

        Loop through role/user mapping and populate self.arc_to_bq_role and
        self.bq_role_to_iam.

        Args:
          valid_roles_to_data_to_arc: is self.valid_roles_to_data_to_arc or
            self.valid_users_to_data_to_arc
          bq_role_count(int): BQ custom role suffix
          arc_map(dict): mapping from all TD ACLs to BigQuery IAM
          arc_composite(dict): a subset of arc_map that contains only composite ACLs

        Returns:
          int: new value of bq_role_count
        """
        for role, data_to_arc in valid_roles_to_data_to_arc.items():
            for data, arc_set in data_to_arc.items():
                arc_abc_str = str(sorted(arc_set))
                if arc_abc_str not in self.arc_to_bq_role:
                    self.arc_to_bq_role[arc_abc_str] = "td2bq_mapper_bqcustom" + str(
                        bq_role_count
                    )
                    self.bq_role_to_iam[
                        "td2bq_mapper_bqcustom" + str(bq_role_count)
                    ] = set()
                    # process composite ACLs first:
                    arc_simple = arc_set.copy()
                    for arc in arc_composite:
                        arc_simple_set = set(arc.split(","))
                        if arc_simple_set.issubset(arc_simple):
                            arc_simple -= arc_simple_set
                            if NA in arc_map[arc]["bq_permissions"]:
                                if arc_abc_str not in self.bq_acl_na:
                                    self.bq_acl_na[arc_abc_str] = {}
                                self.bq_acl_na[arc_abc_str] = {
                                    arc: arc_map[arc]["Error"]
                                }
                                self.bq_role_na.add(role)
                                self.bq_role_to_iam.pop(
                                    "td2bq_mapper_bqcustom" + str(bq_role_count)
                                )
                                break
                            else:
                                self.bq_role_to_iam[
                                    "td2bq_mapper_bqcustom" + str(bq_role_count)
                                ].update(arc_map[arc]["bq_permissions"])
                    # process all other ACLs:
                    for arc in arc_simple:
                        if NA in arc_map[arc]["bq_permissions"]:
                            if arc_abc_str not in self.bq_acl_na:
                                self.bq_acl_na[arc_abc_str] = {}
                            self.bq_acl_na[arc_abc_str] = {arc: arc_map[arc]["Error"]}
                            self.bq_role_na.add(role)
                            self.bq_role_to_iam.pop(
                                "td2bq_mapper_bqcustom" + str(bq_role_count)
                            )
                            break
                        else:
                            self.bq_role_to_iam[
                                "td2bq_mapper_bqcustom" + str(bq_role_count)
                            ].update(arc_map[arc]["bq_permissions"])
                    else:
                        bq_role_count += 1
        return bq_role_count

    def create_arc_to_bq_role_to_iam_map(
        self, arc_file: str, invalid_acls: str
    ) -> bool:
        """Map Teradata ARCs to BigQuery custom roles and IAM.

        Create internal mappings that are used as an input to modules generating
        JSON or Terraform.

        Args:
          arc_file(str): full path to ARC JSON input file.
          invalid_acls(str): full path to a file to log n/a ACL matches

        Returns:
          bool: True on success.
        """
        try:
            with open(arc_file, "r") as arc_map_f:
                arc_map = json.load(arc_map_f)
            arc_composite = self._get_composite_acl(arc_map)

            bq_role_count = self._loop_through_mapping(
                self.valid_roles_to_data_to_arc, 1, arc_map, arc_composite
            )
            self._loop_through_mapping(
                self.valid_users_to_data_to_arc, bq_role_count, arc_map, arc_composite
            )

            if self.bq_acl_na:
                self.create_invalid_report(invalid_acls)
                with open(invalid_acls, "a", newline="") as invalid_f:
                    writer = csv.DictWriter(invalid_f, FIELDS_INVALID_REPORTS)
                    for role_acl in self.bq_acl_na:
                        for role, acl in self.td_role_to_arc_to_data.items():
                            if role_acl in acl.keys():
                                row = {
                                    "RoleName": role,
                                    "AccessRight": role_acl,
                                    "DataName": list(acl.values()),
                                    "ErrorDescription": self.bq_acl_na[role_acl],
                                }
                                writer.writerow(row)
                                self.arc_to_bq_role.pop(role_acl, None)
                        for role, acl in self.td_user_to_arc_to_data.items():
                            if role_acl in acl.keys():
                                row = {
                                    "UserName": role,
                                    "AccessRight": role_acl,
                                    "DataName": list(acl.values()),
                                    "ErrorDescription": self.bq_acl_na[role_acl],
                                }
                                writer.writerow(row)
                                self.arc_to_bq_role.pop(role_acl, None)

        except Exception:
            logger.exception(
                "Could not map Teradata ACL to BigQuery IAM when "
                "creating BigQuery custom roles"
            )
            raise
        return True

    def create_bq_group_to_roles(self) -> bool:
        """Map a BigQuery group to a custom role.

        Mapping is used as input to modules that generate JSON and Terraform

        Returns:
          bool: True on success.
        """
        for group, td_roles in self.td2bq_groups.items():
            if group not in self.bq_group_to_roles:
                self.bq_group_to_roles[group] = set()
            for td_role in td_roles:
                for arc_set_str in self.td_role_to_arc_to_data[
                    td_role
                ].keys():  # look up all td_role's arc sets
                    if arc_set_str in self.arc_to_bq_role:
                        self.bq_group_to_roles[group].add(
                            self.arc_to_bq_role[arc_set_str]
                        )  # find the bq_role associated to each arc set
            if not self.bq_group_to_roles[group]:
                self.bq_group_to_roles.pop(group)
        return True

    def create_bq_role_to_mappings(self) -> bool:
        """Map BigQuery custom roles to IAM permissions, group, and users.

        Mapping of BQ custom roles to IAM permissions, group, and users that are
        assigned to it. Mapping is used as input to modules that generate
        JSON and Terraform.

        Returns:
          bool: True on success.
        """
        for bq_role, iam in self.bq_role_to_iam.items():
            self.bq_role_to_mappings[bq_role] = dict()
            self.bq_role_to_mappings[bq_role]["IAM"] = iam
            self.bq_role_to_mappings[bq_role]["Groups"] = dict()
            self.bq_role_to_mappings[bq_role]["BQ Users"] = dict()

        for arc, bq_role in self.arc_to_bq_role.items():
            self.bq_role_to_mappings[bq_role]["ARC"] = arc

        for group, bq_roles in self.bq_group_to_roles.items():
            for bq_role in bq_roles:
                self.bq_role_to_mappings[bq_role]["Groups"][group] = dict()
                self.bq_role_to_mappings[bq_role]["Groups"][group]["BQ Users"] = set()
                self.bq_role_to_mappings[bq_role]["Groups"][group]["Data"] = set()

        for bq_role in self.bq_role_to_iam.keys():
            arc = self.bq_role_to_mappings[bq_role]["ARC"]

            for group in self.bq_role_to_mappings[bq_role]["Groups"].keys():
                td_roles = set(self.td2bq_groups[group]) - self.bq_role_na
                for td_role in td_roles:
                    for td_user in self.td_role_to_users[td_role]:
                        bq_user = self.td2bq_users[td_user]
                        self.bq_role_to_mappings[bq_role]["Groups"][group][
                            "BQ Users"
                        ].add(bq_user)
                    td_data = self.td_role_to_arc_to_data[td_role][arc]
                    for data in td_data:
                        if "." in data:
                            self.bq_role_to_mappings[bq_role]["Groups"][group][
                                "Data"
                            ].add(self.td2bq_objects[data])
                        else:
                            self.bq_role_to_mappings[bq_role]["Groups"][group][
                                "Data"
                            ].add(self.td2bq_datasets[data])

            if "" in self.td_role_to_users:
                td_users = self.td_role_to_users[""]
                for td_user in td_users:
                    if arc in self.td_user_to_arc_to_data[td_user].keys():
                        bq_role = self.arc_to_bq_role[arc]
                        bq_user = self.td2bq_users[td_user]
                        if bq_user not in self.bq_role_to_mappings[bq_role]["BQ Users"]:
                            self.bq_role_to_mappings[bq_role]["BQ Users"][bq_user][
                                "Data"
                            ] = set()
                        td_data = self.td_user_to_arc_to_data[td_user][arc]
                        for data in td_data:
                            if "." in data:
                                self.bq_role_to_mappings[bq_role]["BQ Users"][bq_user][
                                    "Data"
                                ].add(self.td2bq_objects[data])
                            else:
                                self.bq_role_to_mappings[bq_role]["BQ Users"][bq_user][
                                    "Data"
                                ].add(self.td2bq_datasets[data])
        return True

    def get_bq_role_to_mappings(
        self, change_folder: str, perm_file: str, arc_file: str
    ) -> dict:
        """Create internal mapping representation.

        Create internal mapping representation used to generate JSON files and
        Terraform scripts.

        Args:
          change_folder(str): path to folder to dump files for the user to change
          perm_file(str): full file path of CSV file with exported TD information
          arc_file(str): file path of JSON file with the ARC to IAM permissions map

        Returns:
            bq_role_to_mappings(dict): internal representation mapping for modules
            generating JSON or Terraform
        """
        invalid_reports = os.path.join(change_folder, INVALID_REPORTS_FILE)
        invalid_acls = os.path.join(change_folder, INVALID_ACLS_FILE)
        if os.path.exists(invalid_reports) or os.path.exists(invalid_acls):
            if self.overwrite:
                os.remove(invalid_reports)
                os.remove(invalid_acls)
            else:
                raise ValueError(
                    f"There are files remaining from the previous ACL Mapper run. "
                    f"Please remove those files before running the tool or use "
                    f"--overwrite flag. Files: {INVALID_REPORTS_FILE}, "
                    f"{INVALID_ACLS_FILE}"
                )

        self.convert_users_input(invalid_reports, change_folder)
        self.convert_datasets_input(invalid_reports, change_folder)
        self.convert_objects_input(invalid_reports, change_folder)
        self.convert_groups_input(invalid_reports, change_folder)

        if os.path.exists(invalid_reports):
            raise ValueError(
                f"There is an error in input files. See error details in "
                f"{invalid_reports}. Please change the input files to fix the errors "
                f"and rerun the mapper."
            )

        self.create_td_role_to_users(perm_file)
        self.create_data_to_arc(perm_file)
        # self.valid_*_to_data_to_arc will be removed in the next version
        self.valid_roles_to_data_to_arc = copy.deepcopy(self.td_role_to_data_to_arc)
        self.valid_users_to_data_to_arc = copy.deepcopy(self.td_user_to_data_to_arc)
        self.create_arc_to_data()
        self.create_arc_to_bq_role_to_iam_map(arc_file, invalid_acls)

        # self.catch_invalid_acl(invalid_acls) # Deprecated. Will be removed.

        self.create_bq_group_to_roles()
        self.create_bq_role_to_mappings()

        mod3_map_file = os.path.join(change_folder, BQ_MAPPING_CSV)
        try:
            with open(mod3_map_file, "w") as fp:
                fields = [
                    "BQ Role",
                    "IAM",
                    "ARC",
                    "Groups",
                    "BQ users in the group",
                    "Data for BQ users in group",
                    "BQ users not in the group",
                    "Data for BQ users not in group",
                ]
                writer = csv.DictWriter(fp, fieldnames=fields)
                writer.writeheader()
                for bq_role, mappings in self.bq_role_to_mappings.items():
                    for group, group_info in mappings["Groups"].items():
                        iam_str = ",".join(mappings["IAM"])
                        writer.writerow(
                            {
                                "BQ Role": bq_role,
                                "IAM": re.sub(",", "\n", iam_str),
                                "ARC": re.sub(r"[\[\]']", "", mappings["ARC"]),
                                "Groups": group,
                                "BQ users in the group": ",".join(
                                    group_info["BQ Users"]
                                ),
                                "Data for BQ users in group": ",".join(
                                    group_info["Data"]
                                ),
                            }
                        )
                    for user, user_data in mappings["BQ Users"].items():
                        iam_str = ",".join(mappings["IAM"])
                        writer.writerow(
                            {
                                "BQ Role": bq_role,
                                "IAM": re.sub(",", "\n", iam_str),
                                "ARC": re.sub(r"[\[\]']", "", mappings["ARC"]),
                                "BQ users not in the group": user,
                                "Data for BQ users not in group": ",".join(user_data),
                            }
                        )
        except Exception:
            logger.exception("Could not process file: %s", mod3_map_file)
            raise
        return self.bq_role_to_mappings

    def get_dedupe_roles(self) -> dict:
        """Deduplicate BigQuery custom roles.

        A BigQuery custom role is a duplicate if it has IAM permissions
        that are the some as for some other BigQuery role. This method removes
        the duplicates.
        Creates internal mapping representation used to generate JSON and Terraform
        scripts.

        Returns:
          dedupe_mapping(dict): IAM permissions mapped to their custom roles
        """
        for custom_role, custom_role_info in self.bq_role_to_mappings.items():
            # create mapping of duplicate roles
            iam = repr(custom_role_info["IAM"])
            self.dedupe_mapping.setdefault(iam, []).append(custom_role)

        return self.dedupe_mapping
