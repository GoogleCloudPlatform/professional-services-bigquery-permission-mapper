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

import csv
import logging
import os

# mapping of suggested group name to TD role in user designated folder
CHANGE_FILE_GROUP = "report_groups.csv"

# mapping of TD database to empty BQ dataset names in the user designated folder
CHANGE_FILE_DATASET = "report_datasets.csv"

# mapping of TD object to empty BQ object names in the user designated folder
CHANGE_FILE_OBJECT = "report_objects.csv"

# mapping of TD user to empty BQ user names in the user designated folder
CHANGE_FILE_USER = "report_users.csv"


logger = logging.getLogger("td2bq")


class Td2BqPerm:
    """Instantiates and stores td2bq permissions.

    The class contains methods to export permissions from the database.
    It can also read permissions from the csv file provided by the user.
    Based on the Teradata permissions, the class creates change files that
    the user should fill in to match Teradata users, roles, and objects
    to BigQuery.
    """

    def __init__(self, td_acl_file, users=None, roles=None) -> None:
        """Td2BqPerms instantiates and stores td2bq permissions.

        Args:
            td_acl_file (str): full path to the user's Teradata permissions file
            users (str): user names to export permissions for
            roles (str): role names to export permissions for

        Returns:
            None
        """
        self.internal_perm_file = td_acl_file
        self.td_role_to_data_to_arc = dict()
        self.perm_names = {
            "username",
            "accesstype",
            "rolename",
            "databasename",
            "tablename",
            "columnname",
            "accessright",
        }
        self.users_exclude = """ 'TDPUSER', 'Crashdumps', 'tdwm', 'DBC',
        'LockLogShredder', 'TDMaps', 'Sys_Calendar', 'SysAdmin',
        'SystemFe', 'External_AP' """

        self.query_users = """
        SELECT R.RoleName as Role_Name,
        U.UserName,
        CreatorName,
        CommentString,
        CreateTimeStamp
        FROM DBC.UsersV U
        LEFT JOIN DBC.RoleMembersV R
        ON U.Username = R.Grantee
        WHERE U.Username NOT IN ({users_exclule})
        """.format(
            users_exclule=self.users_exclude
        )

        self.query_perms = """
        select
        UserName (varchar(128))
        ,AccessType (varchar(128))
        ,RoleName (varchar(128)) as Role_Name
        ,DatabaseName (varchar(128))
        ,TableName (varchar(128))
        ,ColumnName (varchar(128))
        ,AccessRight
        ,case
        when accessright='AE' then 'ALTER EXTERNALPROCEDURE'
        when accessright='AF' then 'ALTER FUNCTION'
        when accessright='AP' then 'ALTER PROCEDURE'
        when accessright='AS' then 'ABORT SESSION'
        when accessright='CA' then 'CREATE AUTHORIZATION'
        when accessright='CD' then 'CREATE DATABASE'
        when accessright='CE' then 'CREATE EXTERNAL PROCEDURE'
        when accessright='CF' then 'CREATE FUNCTION'
        when accessright='CG' then 'CREATE TRIGGER'
        when accessright='CM' then 'CREATE MACRO'
        when accessright='CO' then 'CREATE PROFILE'
        when accessright='CP' then 'CHECKPOINT'
        when accessright='CR' then 'CREATE ROLE'
        when accessright='CT' then 'CREATE TABLE'
        when accessright='CU' then 'CREATE USER'
        when accessright='CV' then 'CREATE VIEW'
        when accessright='D' then 'DELETE'
        when accessright='CZ' then 'CREATE ZONE'
        when accessright='DA' then 'DROP AUTHORIZATION'
        when accessright='DD' then 'DROP DATABASE'
        when accessright='DF' then 'DROP FUNCTION'
        when accessright='DG' then 'DROP TRIGGER'
        when accessright='DM' then 'DROP MACRO'
        when accessright='DO' then 'DROP PROFILE'
        when accessright='DP' then 'DUMP'
        when accessright='DR' then 'DROP ROLE'
        when accessright='DT' then 'DROP TABLE'
        when accessright='DU' then 'DROP USER'
        when accessright='DV' then 'DROP VIEW'
        when accessright='E' then 'EXECUTE'
        when accessright='DZ' then 'DROP ZONE'
        when accessright='EF' then 'EXECUTE FUNCTION'
        when accessright='GC' then 'CREATE GLOP'
        when accessright='GD' then 'DROP GLOP'
        when accessright='GM' then 'GLOP MEMBER'
        when accessright='I' then 'INSERT'
        when accessright='IX' then 'INDEX'
        when accessright='MR' then 'MONITOR RESOURCE'
        when accessright='MS' then 'MONITOR SESSION'
        when accessright='NT' then 'NONTEMPORAL'
        when accessright='OD' then 'OVERRIDE DELETE POLICY'
        when accessright='OI' then 'OVERRIDE INSERT POLICY'
        when accessright='OP' then 'CREATE OWNER PROCEDURE'
        when accessright='OS' then 'OVERRIDE SELECT POLICY'
        when accessright='OU' then 'OVERRIDE UPDATE POLICY'
        when accessright='PC' then 'CREATE PROCEDURE'
        when accessright='PD' then 'DROP PROCEDURE'
        when accessright='PE' then 'EXECUTE PROCEDURE'
        when accessright='R' then 'RETRIEVE/SELECT'
        when accessright='RF' then 'REFERENCES'
        when accessright='RS' then 'RESTORE'
        when accessright='SA' then 'SECURITY CONSTRAINT ASSIGNMENT'
        when accessright='SD' then 'SECURITY CONSTRAINT DEFINITION'
        when accessright='ST' then 'STATISTICS'
        when accessright='SS' then 'SET SESSION RATE'
        when accessright='SR' then 'SET RESOURCE RATE'
        when accessright='TH' then 'CTCONTROL'
        when accessright='U' then 'UPDATE'
        when accessright='UU' then 'UDT Usage'
        when accessright='UT' then 'UDT Type'
        when accessright='UM' then 'UDT Method'
        when accessright='ZO' then 'ZONE OVERRIDE'
        else''
        end (varchar(26)) as AccessRightDesc
        ,GrantAuthority
        ,GrantorName (varchar(128))
        ,AllnessFlag
        ,CreatorName (varchar(128))
        ,CreateTimeStamp
        from
        (
        select
        UserName
        ,'User' (varchar(128)) as AccessType
        ,'' (varchar(128)) as RoleName
        ,DatabaseName
        ,TableName
        ,ColumnName
        ,AccessRight
        ,GrantAuthority
        ,GrantorName
        ,AllnessFlag
        ,CreatorName
        ,CreateTimeStamp
        from dbc.allrights
        union all
        select
        Grantee as UserName
        ,'Member' as AccessType
        ,r.RoleName
        ,DatabaseName
        ,TableName
        ,ColumnName
        ,AccessRight
        ,null (char(1)) as GrantAuthority
        ,GrantorName
        ,null (char(1)) as AllnessFlag
        ,null (char(1)) as CreatorName
        ,CreateTimeStamp
        from dbc.allrolerights r
        join dbc.rolemembers m
        on m.RoleName = r.RoleName
        union all
        select
        User as UserName
        ,m.Grantee as AccessType
        ,r.RoleName
        ,DatabaseName
        ,TableName
        ,ColumnName
        ,AccessRight
        ,null (char(1)) as GrantAuthority
        ,GrantorName
        ,null (char(1)) as AllnessFlag
        ,null (char(1)) as CreatorName
        ,CreateTimeStamp
        from dbc.allrolerights r
        join dbc.rolemembers m
        on m.RoleName = r.RoleName
        ) AllRights
        WHERE Username NOT IN ({users_exclule})
        """.format(
            users_exclule=self.users_exclude
        )

        filter = self._get_users_roles(users, roles)
        self.query_perms += filter
        self.query_users += filter

    def _get_users_roles(self, users, roles):
        """Set query string to export users from Teradata.

        Args:
            users (str): users to export from TD
            roles (str): roles to export from TD

        Returns:
            filter (str): filter containing users/roles/None
        """
        users_include = " and Username in ({})"
        roles_include = " and Role_Name in ({})"

        filter = ""
        if users is not None:
            try:
                users_str = ",".join(
                    "'{}'".format(x) for x in users.replace(" ", ",").split(",") if x
                )
                filter += users_include.format(users_str)
            except Exception:
                logger.exception("Cannot parse user list.")
                raise
        if roles is not None:
            try:
                roles_str = ",".join(
                    "'{}'".format(x) for x in roles.replace(" ", ",").split(",") if x
                )
                filter += roles_include.format(roles_str)
            except Exception:
                logger.exception("Cannot parse role list.")
                raise
        return filter

    def get_query_users(self) -> str:
        """Get query string to export users from Teradata."""
        return self.query_users

    def get_query_perms(self) -> str:
        """Get query string to export permissions from Teradata."""
        return self.query_perms

    def is_valid_perm(self, columns):
        """Check if input list of permissions contains all required columns.

        Args:
            columns (str): list of names which represents TD permission info

        Returns:
            bool: True is the input list contains all required names.
        """
        return self.perm_names.issubset(set([x.lower().strip() for x in columns]))

    def get_column_names(self) -> set:
        """Gets list of required column names in input permissions CSV file."""
        return self.perm_names

    def _acl_valid(self, row: dict, invalid_acl: dict) -> bool:
        """Returns True if input row contains objects with valid permissions.

        Args:
          row: a row from self.internal_perm_file
          invalid_acl: contains users, roles, and objects that have invalid n/a
            permissions.

        Returns:
          True only if all entries in an input row are valid, i.e. don't include
          n/a permissions.
        """
        if not invalid_acl:
            return True
        obj_name = row["TableName"].strip() if row["TableName"] != "All" else ""
        if (
            row["UserName"].strip() not in invalid_acl.get("users", [])
            and row["RoleName"].strip() not in invalid_acl.get("roles", [])
            and row["DatabaseName"].strip() + "." + obj_name
            not in invalid_acl.get("objects", [])
        ):
            return True
        else:
            return False

    def create_td2bq_name_change_files_csv(
        self, change_folder: str, invalid_acl: dict = None
    ) -> None:
        """Create user change files.

        Creates 3 mapping files in CSV format as output to the user to change the
        user, database, and object names.

        This method create files:
          CHANGE_FILE_DATASET
          CHANGE_FILE_OBJECT
          CHANGE_FILE_USER

        Args:
          change_folder(str): path to folder to dump files for the user to change
          invalid_acl(dict): TC users, roles, and objects that contain n/a ACLs

        Returns:
          None
        """
        td_users = set()
        td_databases = set()
        td_objects = set()

        try:
            with open(self.internal_perm_file, "r") as perms_f:
                reader = csv.DictReader(perms_f)
                for row in reader:
                    if self._acl_valid(row, invalid_acl):
                        td_users.add(row["UserName"].strip())
                        td_databases.add(row["DatabaseName"].strip())
                        if row["TableName"] != "All":
                            td_objects.add(
                                row["DatabaseName"].strip()
                                + "."
                                + row["TableName"].strip()
                            )
        except Exception:
            logger.exception("Could not process file: %s", self.internal_perm_file)
            raise

        td2bq_user_file = os.path.join(change_folder, CHANGE_FILE_USER)
        try:
            with open(td2bq_user_file, "w", newline="") as user_fp:
                fieldnames = ["Teradata_user", "GCP_user"]
                user_writer = csv.DictWriter(user_fp, fieldnames=fieldnames)
                user_writer.writeheader()
                for user in td_users:
                    user_writer.writerow({"Teradata_user": user})
        except Exception:
            logger.exception("Could not process file: %s", td2bq_user_file)
            raise
        logger.info(
            "Created %s file. Fill it in before generating JSON " "or Terraform",
            td2bq_user_file,
        )

        td2bq_dataset_file = os.path.join(change_folder, CHANGE_FILE_DATASET)
        try:
            with open(td2bq_dataset_file, "w", newline="") as dataset_fp:
                fieldnames = ["Teradata_database", "BigQuery_dataset"]
                dataset_writer = csv.DictWriter(dataset_fp, fieldnames=fieldnames)
                dataset_writer.writeheader()
                for database in td_databases:
                    dataset_writer.writerow({"Teradata_database": database})
        except Exception:
            logger.exception("Could not process file: %s", td2bq_dataset_file)
            raise
        logger.info(
            "Created %s file. Fill it in before generating JSON " "or Terraform",
            td2bq_dataset_file,
        )

        td2bq_object_file = os.path.join(change_folder, CHANGE_FILE_OBJECT)
        try:
            with open(td2bq_object_file, "w", newline="") as object_fp:
                fieldnames = ["Teradata_object", "BigQuery_object"]
                object_writer = csv.DictWriter(object_fp, fieldnames=fieldnames)
                object_writer.writeheader()
                for table in td_objects:
                    object_writer.writerow({"Teradata_object": table})
        except Exception:
            logger.exception("Could not process file: %s", td2bq_object_file)
            raise
        logger.info(
            "Created %s file. Fill it in before generating JSON " "or Terraform",
            td2bq_object_file,
        )

    def create_td_role_to_data_to_arc(self, invalid_acl: dict = {}):
        """Create mapping of TD roles to BQ groups.

        Internal mapping used in the creation of the TD role to BQ group map as
        output to the user.

        Args:
            invalid_acl(dict): TD users, roles, and objects that cannot be mapped
            to BQ IAM. I.e. they contain n/a permissions
        """
        try:
            with open(self.internal_perm_file, "r") as perms_f:
                reader = csv.DictReader(perms_f)
                for row in reader:
                    if self._acl_valid(row, invalid_acl):
                        td_role = row["RoleName"].strip()
                        td_database = row["DatabaseName"].strip()
                        td_object = row["TableName"].strip()
                        arc = row["AccessRight"].strip()

                        if td_role != "":  # user assigned a role
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
                                self.td_role_to_data_to_arc[td_role][td_database].add(
                                    arc
                                )
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
            logger.exception("Could not process file: %s", self.internal_perm_file)
            raise

    def create_user_groups_csv(self, change_folder: str):
        """Create user groups CSV file.

        Find duplicate TD roles and create user groups for all TD roles as
        output file to the user.

        Creates:
          CHANGE_FILE_GROUP

        Args:
            change_folder(str): full path to folder to dump group file for the
            user to change

        Returns:
          None
        """
        group_to_td_role_to_renamed_group = dict()
        claimed_roles = set()  # roles that have been checked and is a duplicate
        group_count = 1

        # finds the duplicate TD roles and assigns to incremented group name
        for td_role, data_to_arc in self.td_role_to_data_to_arc.items():
            if td_role not in claimed_roles:
                for td_role_2, data_to_arc_2 in self.td_role_to_data_to_arc.items():
                    if td_role_2 not in claimed_roles:
                        if td_role != td_role_2:
                            if len(data_to_arc) == len(data_to_arc_2):
                                if set(data_to_arc.keys()) == set(data_to_arc_2.keys()):
                                    for data in data_to_arc.keys():
                                        # if the ARCs do not match for the dataset/object
                                        if data_to_arc[data] != data_to_arc_2[data]:
                                            break
                                    group_to_td_role_to_renamed_group[
                                        "group" + str(group_count) + "@{%domain.com%}"
                                    ] = dict()
                                    group_to_td_role_to_renamed_group[
                                        "group" + str(group_count) + "@{%domain.com%}"
                                    ][td_role] = ""
                                    group_to_td_role_to_renamed_group[
                                        "group" + str(group_count) + "@{%domain.com%}"
                                    ][td_role_2] = ""
                                    claimed_roles.add(
                                        td_role_2
                                    )  # role marked as duplicate
                                    claimed_roles.add(
                                        td_role
                                    )  # role marked as duplicate
                                    group_count += 1

        # for all other roles that are not duplicates,
        # each role is assigned to own group
        for td_role in self.td_role_to_data_to_arc.keys():
            if td_role not in claimed_roles:
                group_to_td_role_to_renamed_group[
                    "group" + str(group_count) + "@{%domain.com%}"
                ] = dict()
                group_to_td_role_to_renamed_group[
                    "group" + str(group_count) + "@{%domain.com%}"
                ][td_role] = ""
                claimed_roles.add(td_role)  # role marked as claimed (it's own group)
                group_count += 1

        groups_file = os.path.join(change_folder, CHANGE_FILE_GROUP)
        try:
            with open(groups_file, "w", newline="") as group_fp:
                fieldnames = [
                    "Teradata_role",
                    "GCP_user_group",
                    "Rename_GCP_user_group",
                ]
                writer = csv.DictWriter(group_fp, fieldnames=fieldnames)
                writer.writeheader()
                for group, td_role_renamed in group_to_td_role_to_renamed_group.items():
                    for td_role, renamed in td_role_renamed.items():
                        writer.writerow(
                            {
                                "Teradata_role": td_role,
                                "GCP_user_group": group,
                                "Rename_GCP_user_group": renamed,
                            }
                        )
        except Exception:
            logger.exception("Could not process file: %s", groups_file)
            raise
        logger.info(
            "Created %s file. Fill it in before generating JSON " "or Terraform",
            groups_file,
        )
