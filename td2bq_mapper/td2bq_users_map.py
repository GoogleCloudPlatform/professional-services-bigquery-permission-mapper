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

import logging
from typing import Union

from .td2bq_util import get_root_dir, write_json_file

logger = logging.getLogger("td2bq")


class Td2BqUsersMap:
    """
    Td2BqUserMapper class.  used to instantiate and store
    td2bq_permissions_users_map
    """

    def __init__(self) -> None:
        """
        Td2BqUsersMapper constructor. Initialize class variables
        Args: None

        Returns:
            None
        """
        # defaults
        self.td2bq_users_map = None
        # self.map3_generated_file = get_root_dir() + "/data/td2bq_map3_generated_map.dict"

        # terraform files
        self.generated_tf_folder = get_root_dir() + "/../terraform_generated/"
        self.generated_tf_groups_file = self.generated_tf_folder + "iam_groups.tf.json"
        self.generated_tf_custom_roles_file = (
            self.generated_tf_folder + "iam_custom_roles.tf.json"
        )
        self.generated_tf_invalid_custom_roles_file = (
            self.generated_tf_folder + "iam_invalid_custom_roles.tf.json"
        )
        self.generated_tf_datasets_file = (
            self.generated_tf_folder + "iam_datasets_roles_binding.tf.json"
        )
        self.generated_tf_tables_file = (
            self.generated_tf_folder + "iam_tables_roles_binding.tf.json"
        )

    def keys_changed(self, original: dict, updated: dict) -> bool:
        """
        Check if user changed/updated any of the keys in dictionary.
        Args:
            original(dictionary): original dictionary provided to user
            updated(dictionary): updated dictionary by user.

        Returns:
            True if keys where changed. False otherwise.
        """
        result = False
        for (k1, v1) in original.items():
            v2 = updated.get(k1)
            k2 = k1
            if v2 is None:
                k2 = " "
                logger.info(f"Dictionary key {k2} has changed. Should be {k1}")
                return True

            if isinstance(v1, dict):
                result = self.keys_changed(v1, v2)
            if k1 != k2:
                logger.info(f"Dictionary key {k2} has changed. Should be {k1}")
                result = True
        return result

    def get_group_dict(self, group: str, cid: str) -> dict:
        """
        Generate the Google group Terraform JSON config section
        Args:
            group(string): Google Group name to create i.e. group1@myorg.com
            cid(string): The Customer ID for the org. i.e C03gcxvlm

        Returns:
            group_dict(dict): Terraform JSON group dictionary.
        """
        group_dict = {
            "display_name": group.split("@")[0],
            "parent": "customers/" + cid,
            "group_key": {"id": group},
            "labels": {"cloudidentity.googleapis.com/groups.discussion_forum": ""},
        }
        return group_dict

    def get_group_membership(self, group: str) -> dict:
        """
        Generate the google group members terraform json config section
        Args:
            group(string): Google Group name to create i.e. group1@myorg.com

        Returns:
            group_dict(dict): Terraform JSON group member dictionary.
        """

        group_name = group.split("@")[0]
        _ = group_name + "_var"  # gvar
        mem_dict = {
            "for_each": f"${{toset(local.{group_name}.emails)}}",
            "group": f"${{google_cloud_identity_group.{group_name}.id}}",
            "preferred_member_key": {"id": "${each.key}"},
            "roles": {"name": "MEMBER"},
        }
        return mem_dict

    def get_groups_from_bq_role_to_mappings(
        self, bq_role_to_mappings: dict
    ) -> Union[dict, None]:
        """
        From previous step's output dictionary file extract the groups and users
        Args:
            bq_role_to_mappings: previous step's output dictionary

        Returns:
            groups_map(dict): dictionary with groups/users map on success.
                              None on failure.
        """
        group_map = {}
        for custom_role, custom_role_info in bq_role_to_mappings.items():
            for group, group_users_data in custom_role_info["Groups"].items():
                if group not in group_map:
                    group_map[group] = set()
                for user in group_users_data["BQ Users"]:
                    if user not in group_map[group]:
                        group_map[group].add(user)

        # TODO Check for empty groups or empty user sets if those are possible.
        return group_map

    def gen_tf_iam_groups(
        self, bq_role_to_mappings: dict, cid: str, project_id: str
    ) -> bool:
        """
        Generate Terraform script that will create Google groups
        Args:
            bq_role_to_mappings(dict): bq_role_to_mappings mapping data
              structure
            cid(str): The customer ID to use i.e. C02dfkjhd
            project_id(str): Project ID

        Returns:
            bool: True on success. False on failure
        """
        td2bq_groups_map = self.get_groups_from_bq_role_to_mappings(bq_role_to_mappings)

        # load tf template into dictionary
        tf_template = {
            "provider": {
                "google": {"billing_project": project_id, "user_project_override": True}
            },
            "locals": {},
            "resource": {
                "google_cloud_identity_group": {},
                "google_cloud_identity_group_membership": {},
            },
        }

        for group, users in td2bq_groups_map.items():
            group_name = group.split("@")[0]

            fname = f"iam_email_list_{group_name}.json"
            fn = f'${{jsondecode(file("${{path.module}}/{fname}"))}}'
            tf_template["locals"][group_name] = fn

            # write out email list
            emails = {"emails": list(users)}

            ef_name = self.generated_tf_folder + fname
            logger.info("paths=", ef_name, self.generated_tf_folder)
            if not write_json_file(ef_name, emails):
                logger.info("Problem writing group email file")
                return False

            # update the rest of the tf_template
            tf_group = self.get_group_dict(group, cid)
            tf_template["resource"]["google_cloud_identity_group"][
                group_name
            ] = tf_group

            tf_member = self.get_group_membership(group)
            member_name = group_name + "_member"
            tf_template["resource"]["google_cloud_identity_group_membership"][
                member_name
            ] = tf_member

        if not write_json_file(self.generated_tf_groups_file, tf_template):
            logger.info("Problem writing IAM group file")
            return False
        return True

    def gen_tf_iam_custom_roles(
        self, bq_role_to_mappings: dict, dedupe_mapping: dict, project_id: str
    ) -> bool:
        """
        Generate Terraform script that will create IAM custom roles. Valid roles
        are placed in one file while invalid
        roles (permission is n/a) is put in another file. Valid roles will also
        be deduplicated (roles with same IAM
        permissions will be combined into one custom role).
        Args:
            bq_role_to_mappings(dict): bq_role_to_mappings mapping data
              structure
            dedupe_mapping(dict): IAM permissions mapped to their custom roles
            project_id(str): Project ID

        Returns:
            bool: True on success. False on failure
        """
        valid_roles = dict()
        invalid_roles = dict()

        for iam, dupe_roles in dedupe_mapping.items():
            if "n/a" in iam:
                for role in dupe_roles:
                    invalid_roles[role] = {
                        "role_id": role,
                        "title": role,
                        "permissions": list(eval(iam)),
                        "project": project_id,
                    }
            else:
                valid_roles[dupe_roles[0]] = {
                    "role_id": dupe_roles[0],
                    "title": dupe_roles[0],
                    "permissions": list(eval(iam)),
                    "project": project_id,
                }

        # for custom_role, custom_role_info in bq_role_to_mappings.items():
        #     if "n/a" in custom_role_info["IAM"]:
        #         invalid_roles[custom_role] = {
        #             "role_id": custom_role,
        #             "title": custom_role,
        #             "permissions": list(custom_role_info["IAM"]),
        #             "project": project_id
        #         }
        #     else:
        #         valid_roles[custom_role] = {
        #             "role_id": custom_role,
        #             "title": custom_role,
        #             "permissions": list(custom_role_info["IAM"]),
        #             "project": project_id
        #         }

        invalid_tf_template = {
            "resource": {"google_project_iam_custom_role": invalid_roles}
        }
        valid_tf_template = {
            "resource": {"google_project_iam_custom_role": valid_roles}
        }

        if not write_json_file(self.generated_tf_custom_roles_file, valid_tf_template):
            logger.info("Problem writing IAM custom roles file")
            return False
        if not write_json_file(
            self.generated_tf_invalid_custom_roles_file, invalid_tf_template
        ):
            logger.info("Problem writing IAM invalid custom roles file")
            return False

        return True

    def gen_tf_bq_table_dataset_iam_binding(
        self, bq_role_to_mappings: dict, dedupe_mapping: dict, project_id: str
    ) -> bool:
        """
        Generate Terraform script that will create binding of IAM custom roles
        to users/groups, and BQ datasets/tables.
        Args:
            bq_role_to_mappings(dict): bq_role_to_mappings mapping data
              structure
            dedupe_mapping(dict): IAM permissions mapped to their custom roles
            project_id(str): Project ID

        Returns:
            bool: True on success. False on failure
        """
        # load tf template into dictionary
        tf_template_dataset = {"resource": {"google_bigquery_dataset_iam_member": {}}}

        tf_template_table = {"resource": {"google_bigquery_table_iam_member": {}}}

        # grant roles to members (groups/users)
        for custom_role, custom_role_info in bq_role_to_mappings.items():
            if custom_role_info["Groups"]:
                for group, group_info in custom_role_info["Groups"].items():
                    g_name = group.split("@")[0]
                    for data in group_info["Data"]:
                        d_name = data.replace(".", "_")
                        member = f"{g_name}_{d_name}"
                        binding = {member: {}}
                        if "n/a" not in custom_role_info["IAM"]:
                            role = dedupe_mapping[repr(custom_role_info["IAM"])][0]
                            binding[member][
                                "role"
                            ] = f"projects/{project_id}/roles/{role}"
                        else:
                            binding[member][
                                "role"
                            ] = f"projects/{project_id}/roles/{custom_role}"
                        binding[member]["member"] = f"group:{group}"
                        binding[member]["project"] = project_id

                        if "." in data:  # this is a dataset + table
                            data_info = data.split(".")
                            binding[member]["dataset_id"] = data_info[0]
                            binding[member]["table_id"] = data_info[1]
                            tf_template_table["resource"][
                                "google_bigquery_table_iam_member"
                            ][member] = binding[member]
                        else:  # this is a dataset
                            binding[member]["dataset_id"] = data
                            tf_template_dataset["resource"][
                                "google_bigquery_dataset_iam_member"
                            ][member] = binding[member]
            if custom_role_info["BQ Users"]:
                for user, user_info in custom_role_info["BQ Users"].items():
                    u_name = user.split("@")[0]
                    for data in user_info["Data"]:
                        d_name = data.replace(".", "_")
                        member = f"{u_name}_{d_name}"
                        binding = {member: {}}
                        if "n/a" not in custom_role_info["IAM"]:
                            role = dedupe_mapping[repr(custom_role_info["IAM"])][0]
                            binding[member][
                                "role"
                            ] = f"projects/{project_id}/roles/{role}"
                        else:
                            binding[member][
                                "role"
                            ] = f"projects/{project_id}/roles/{custom_role}"
                        binding[member]["member"] = f"user:{user}"
                        binding[member]["project"] = project_id

                        if "." in data:  # this is a dataset + table
                            data_info = data.split(".")
                            binding[member]["dataset_id"] = data_info[0]
                            binding[member]["table_id"] = data_info[1]
                            tf_template_table["resource"][
                                "google_bigquery_table_iam_member"
                            ][member] = binding[member]
                        else:  # this is a dataset
                            binding[member]["dataset_id"] = data
                            tf_template_dataset["resource"][
                                "google_bigquery_dataset_iam_member"
                            ][member] = binding[member]

        if tf_template_table["resource"]["google_bigquery_table_iam_member"]:
            if not write_json_file(self.generated_tf_tables_file, tf_template_table):
                logger.info("Problem writing IAM group to BQ table binding file")
                return False
        if tf_template_dataset["resource"]["google_bigquery_dataset_iam_member"]:
            if not write_json_file(
                self.generated_tf_datasets_file, tf_template_dataset
            ):
                logger.info("Problem writing IAM group to BQ dataset binding file")
                return False

        return True
