# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Constant values used by the mapper."""

# File names
ACCESS_MAP_INPUT_CSV = "access_map_input.csv"
PREDEFINED_ARC_MAP_JSON = "predefined_arc_map.json"


# Define input sheet column names - Teradata
TD_ROLE = "Role"
TD_DATABASE = "DatabaseName"
TD_ACCESS_RIGHT = "AccessRight"
TD_TABLE_NAME = "TableName"


# Define input sheet column names - GCP
GCP_GROUP = "GCPGroupName"
GCP_PROJECT = "GCPProjectId"
BQ_DATASET = "BQDatasetName"
BQ_TABLE = "BQTableName"
IAM_ROLE = "IAMRole"

ALL = "All"  # Identifier for dataset-level access

COLUMNS = [
    TD_ROLE,
    TD_DATABASE,
    TD_ACCESS_RIGHT,
    TD_TABLE_NAME,
    GCP_GROUP,
    GCP_PROJECT,
    BQ_DATASET,
    BQ_TABLE,
    IAM_ROLE,
]

# Information audit log strings used in mapping generation
INHERITED_FROM_DS = "MAPPER:ACCESS_INHERITED_FROM_DATASET"
DUPLICATE_GRANT = "MAPPER:DUPLICATE_ROLE_ALREADY_GRANTED"

# Role priority mapping to determine replacement
ROLE_HIERARCHY = {
    "roles/bigquery.dataAdmin": 1,
    "roles/bigquery.dataEditor": 2,
    "roles/bigquery.dataViewer": 3,
    INHERITED_FROM_DS: 0,
    "NOT_APPLICABLE": 0,
}
