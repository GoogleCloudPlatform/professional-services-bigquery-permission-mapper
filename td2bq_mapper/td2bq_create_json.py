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

import json
import logging
import os

from td2bq_mapper import td2bq_util

logger = logging.getLogger("td2bq")


class Td2BqJson:
    """Class to create JSON files with BigQuery IAM bindings and custom roles."""

    def __init__(self, change_folder: str) -> None:
        """Td2BqJson constructor. Initialize class variables.

        Args:
          change_folder: directory to create change files

        Returns:
          None
        """
        # json files
        if os.path.isdir(change_folder):
            self.json_folder = os.path.join(change_folder, "json_generated/")
            self.custom_roles_folder = os.path.join(self.json_folder, "custom_roles")
            # self.invalid_roles_folder = os.path.join(self.json_folder,
            #                                          "custom_roles/invalid/")
            # self.member_role_binding_file = os.path.join(self.json_folder,
            #                                              "member_role_binding.json")
            self.datasets_folder = os.path.join(self.json_folder, "datasets")
            self.tables_folder = os.path.join(self.json_folder, "tables")
        else:
            raise ValueError(f"Path {change_folder} is not an existing directory.")

    def create_json_iam_custom_roles(self, dedupe_mapping: dict) -> bool:
        """Create individual JSON files for each BigQuery custom role.

        Create individual JSON files for each custom role to be created with
        gcloud iam roles create

        Args:
          dedupe_mapping(dict): IAM permissions mapped to their custom roles

        Returns:
          bool: True on success. False on failure to create JSON file(s)
        """
        result = True
        for iam, dupe_roles in dedupe_mapping.items():
            if "n/a" not in iam:
                json_custom_role = dict()
                unique_role = dupe_roles[0]
                json_custom_role[unique_role] = dict()
                json_custom_role[unique_role] = {
                    "title": unique_role,
                    "includedPermissions": list(eval(iam)),
                    "stage": "ALPHA",
                }
                # make_dirs(self.custom_roles_folder, overwrite)
                if not td2bq_util.write_json_file(
                    os.path.join(self.custom_roles_folder, unique_role + ".json"),
                    json_custom_role[unique_role],
                ):
                    logger.error(
                        "Error, couldn't write IAM custom roles JSON file " "for: %s",
                        unique_role,
                    )
                    result = False
            else:
                logger.error("n/a is encountered in role: %s, iam: %s", dupe_roles, iam)
                result = False
        return result

    def create_policy_bindings(
        self, bq_role_to_mappings: dict, dedupe_mapping: dict, project_id: str
    ):
        """Generates a JSON file with bindings. Deprecated.

        Generates a JSON file with bindings between the newly created custom
        roles to users/groups with gcloud projects set-iam-policy.
        This method is deprecated and will be removed in future version.

        Args:
          bq_role_to_mappings(dict): bq_role_to_mappings mapping data structure
          dedupe_mapping(dict): IAM permissions mapped to their custom roles
          project_id(str): Project ID

        Returns:
          bool: True on success. False on failure
        """
        bindings = {"bindings": list()}

        for iam, dupe_roles in dedupe_mapping.items():
            if "n/a" not in iam:
                actual_bindings = {
                    "role": "projects/" + project_id + "/roles/" + dupe_roles[0],
                    "members": [],
                }
                for role in dupe_roles:
                    for group in bq_role_to_mappings[role]["Groups"].keys():
                        actual_bindings["members"].append("group:" + group)
                    for bq_user in bq_role_to_mappings[role]["BQ Users"].keys():
                        actual_bindings["members"].append("user:" + bq_user)
                bindings["bindings"].append(actual_bindings)
            else:
                for role in dupe_roles:
                    actual_bindings = {
                        "role": "projects/" + project_id + "/roles/" + role,
                        "members": [],
                    }
                    for group in bq_role_to_mappings[role]["Groups"].keys():
                        actual_bindings["members"].append("group:" + group)
                    for bq_user in bq_role_to_mappings[role]["BQ Users"].keys():
                        actual_bindings["members"].append("user:" + bq_user)
                    bindings["bindings"].append(actual_bindings)
        # Deprecated code. We don't create this file any more.
        # if not write_json_file(self.member_role_binding_file, bindings):
        #   logger.error(f"Error, Problem writing groups and users "
        #                f"to custom roles JSON file: {bindings}")
        #   return False
        return True

    def write_json(self, new_data: dict, filename: str, key: str) -> bool:
        """Helper function to write to existing JSON file.

        Args:
          new_data(dict): new data to be added to file
          filename(str): name of the file to update
          key(str): either "bindings" for table JSON key or "access" for dataset
            JSON key

        Returns:
          bool: True on success. False on failure
        """
        try:
            with open(filename, "r+") as file:
                file_data = json.load(file)  # Load existing data into a dict.
                file_data[key].append(new_data)  # Join new_data with file_data
                file.seek(0)  # Sets file's current position at offset.
                json.dump(file_data, file, indent=4)  # convert back to json.
        except Exception:
            logger.exception("Error, could not upend to JSON file: %s", filename)
            return False
        return True

    def create_dataset_role_json_file(
        self, bq_role_to_mappings: dict, dedupe_mapping: dict, project_id: str
    ) -> bool:
        """Creates JSON files for each dataset and table.

        Creates JSON files for each dataset and table to bind to members and roles

        Args:
          bq_role_to_mappings(dict): bq_role_to_mappings mapping data structure
          dedupe_mapping(dict): IAM permissions mapped to their custom roles
          project_id(str): project ID which the dataset/table belongs to

        Returns:
          bool: True on success. False on failure
        """
        files_created = []  # track what files were created during this run
        new_dataset_binding = {
            "access": [  # template for dataset binding
                {"role": "WRITER", "specialGroup": "projectWriters"},
                {"role": "OWNER", "specialGroup": "projectOwners"},
                {"role": "READER", "specialGroup": "projectReaders"},
            ]
        }
        result = True
        for iam, dupe_roles in dedupe_mapping.items():
            for role in dupe_roles:
                for group, group_info in bq_role_to_mappings[role]["Groups"].items():
                    for data in group_info["Data"]:
                        if "." in data:  # resource is a table
                            new_binding = {
                                "members": ["group:" + group],
                            }
                            if "n/a" not in iam:
                                new_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + dupe_roles[0]
                                )
                            else:
                                new_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + role
                                )

                            file = os.path.join(self.tables_folder, data + ".json")
                            if file not in files_created:
                                files_created.append(file)
                                if not td2bq_util.write_json_file(file, new_binding):
                                    logger.error(
                                        "Error writing new access bindings to "
                                        "table JSON file: %s",
                                        file,
                                    )
                                    result = False
                            elif not self.write_json(new_binding, file, "bindings"):
                                result = False
                        else:  # resource is a dataset
                            file = os.path.join(self.datasets_folder, data + ".json")
                            if file not in files_created:
                                files_created.append(file)
                                if not td2bq_util.write_json_file(
                                    file, new_dataset_binding
                                ):
                                    logger.error(
                                        "Error writing new access bindings to dataset "
                                        "JSON file: %s",
                                        file,
                                    )
                                    result = False
                            add_binding = {
                                "groupByEmail": group,
                                "role": "projects/"
                                + project_id
                                + "/roles/"
                                + dupe_roles[0],
                            }
                            if "n/a" not in iam:
                                add_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + dupe_roles[0]
                                )
                            else:
                                add_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + role
                                )
                            if not self.write_json(add_binding, file, "access"):
                                result = False

                for bq_user, datas in bq_role_to_mappings[role]["BQ Users"].items():
                    for data in datas:
                        if "." in data:  # resource is a table
                            new_binding = {
                                "members": ["user:" + bq_user],
                            }
                            if "n/a" not in iam:
                                new_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + dupe_roles[0]
                                )
                            else:
                                new_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + role
                                )

                            file = os.path.join(self.tables_folder, data + ".json")
                            if file not in files_created:
                                files_created.append(file)
                                if not td2bq_util.write_json_file(file, new_binding):
                                    logger.error(
                                        "Error writing new access bindings to table "
                                        "JSON file: %s",
                                        file,
                                    )
                                    result = False
                            elif not self.write_json(new_binding, file, "bindings"):
                                result = False
                        else:  # resource is a dataset
                            file = os.path.join(self.datasets_folder, data + ".json")
                            if file not in files_created:
                                files_created.append(file)
                                if not td2bq_util.write_json_file(
                                    file, new_dataset_binding
                                ):
                                    logger.error(
                                        "Error writing new access bindings to "
                                        "dataset JSON file: %s",
                                        file,
                                    )
                                    result = False
                            add_binding = {
                                "userByEmail": bq_user,
                            }
                            if "n/a" not in iam:
                                add_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + dupe_roles[0]
                                )
                            else:
                                add_binding["role"] = (
                                    "projects/" + project_id + "/roles/" + role
                                )

                            if not self.write_json(add_binding, file, "access"):
                                result = False
        return result
