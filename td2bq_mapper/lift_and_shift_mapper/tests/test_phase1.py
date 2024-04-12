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

"""Contains tests for the lift-and-shift predefined role mapper."""

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from td2bq_mapper.lift_and_shift_mapper import consts, ls_phase1_gen_map

# Role priority mapping used by tests. Any changes must also be made in consts.ROLE_HIERARCHY.
TEST_ROLE_HIERARCHY = {
    "admin": 100,
    "editor": 200,
    "viewer": 300,
    consts.INHERITED_FROM_DS: 999,
    consts.OVERLAP_GRANT: 999,
    consts.DUPLICATE_GRANT: 999,
    "NOT_APPLICABLE": 999,
}

TEST_COLUMNS = [
    consts.GCP_GROUP,
    consts.GCP_PROJECT,
    consts.BQ_DATASET,
    consts.BQ_TABLE,
    consts.IAM_ROLE,
]

# Options to allow printing full dataframes when debugging
pd.set_option("display.max_columns", 1000)
pd.set_option("display.width", 1000)


def test_dataset_inheritance_dedupe():
    """If tables for a given group have permissions that would already be inherited from dataset-level bindings, remove the table-level ones."""
    input_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "editor"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl2", "admin"],
            ["group1@example.com", "proj1", "ds2", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "editor"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "viewer"],
            ["group1@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds2", "tbl2", "viewer"],
        ],
        columns=TEST_COLUMNS,
    )
    expected_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "editor"],
            [
                "group1@example.com",
                "proj1",
                "ds1",
                "tbl1",
                consts.INHERITED_FROM_DS,
            ],
            ["group1@example.com", "proj1", "ds1", "tbl2", "admin"],
            ["group1@example.com", "proj1", "ds2", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "editor"],
            [
                "group1@example.com",
                "proj1",
                "ds2",
                "tbl2",
                consts.INHERITED_FROM_DS,
            ],
            ["group1@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds2", "tbl2", "viewer"],
        ],
        columns=TEST_COLUMNS,
    )

    result_df = input_df.copy()
    result_df = ls_phase1_gen_map.dedupe_dataset_iam_inheritance(
        result_df, TEST_ROLE_HIERARCHY
    )
    assert_frame_equal(expected_df, result_df)


def test_table_role_dedupe():
    """Leave only one row for a given IAM role for a given table-group when the same role is granted multiple times."""
    input_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds1", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "editor"],
            ["group1@example.com", "proj1", "ds1", "tbl2", "admin"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "editor"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "viewer"],
            ["group1@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds2", "tbl2", "viewer"],
        ],
        columns=TEST_COLUMNS,
    )
    expected_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds1", consts.ALL, consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds1", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "editor"],
            ["group1@example.com", "proj1", "ds1", "tbl2", "admin"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "editor"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "viewer"],
            ["group1@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds2", "tbl2", "viewer"],
        ],
        columns=TEST_COLUMNS,
    )
    result_df = input_df.copy()
    result_df = ls_phase1_gen_map.dedupe_identical_iam_role_grants(result_df)
    assert_frame_equal(expected_df, result_df)


def test_role_overlap_within_obj_dedupe():
    """Remove all but the most permissive role (based on the role hierarchy) from a given dataset or table."""
    input_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "editor"],
            ["group1@example.com", "proj1", "ds1", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds1", consts.ALL, "admin"],
            ["group1@example.com", "proj1", "ds2", "tbl1", "editor"],
            ["group1@example.com", "proj1", "ds2", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds2", "tbl1", "viewer"],
        ],
        columns=TEST_COLUMNS,
    )
    expected_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, consts.OVERLAP_GRANT],
            ["group1@example.com", "proj1", "ds1", consts.ALL, consts.OVERLAP_GRANT],
            ["group1@example.com", "proj1", "ds1", consts.ALL, "admin"],
            ["group1@example.com", "proj1", "ds2", "tbl1", consts.OVERLAP_GRANT],
            ["group1@example.com", "proj1", "ds2", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds2", "tbl1", consts.OVERLAP_GRANT],
        ],
        columns=TEST_COLUMNS,
    )

    result_df = input_df.copy()
    result_df = ls_phase1_gen_map.dedupe_overlap_higher_iam_grant(
        result_df, TEST_ROLE_HIERARCHY
    )
    assert_frame_equal(expected_df, result_df)


def test_dedupe_identical_then_ds_inheritance_then_resource_overlap():
    """First mark identical IAM grants for the same resource, then mark permissions inherited from the dataset-level, then mark role overlaps for a given resource."""
    input_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "editor"],
            ["group1@example.com", "proj1", "ds1", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "editor"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
        ],
        columns=TEST_COLUMNS,
    )
    expected_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "editor"],
            ["group1@example.com", "proj1", "ds1", consts.ALL, consts.OVERLAP_GRANT],
            ["group1@example.com", "proj1", "ds1", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.INHERITED_FROM_DS],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.INHERITED_FROM_DS],
        ],
        columns=TEST_COLUMNS,
    )

    result_df = input_df.copy()
    result_df = ls_phase1_gen_map.dedupe_identical_iam_role_grants(result_df)
    result_df = ls_phase1_gen_map.dedupe_dataset_iam_inheritance(
        result_df, TEST_ROLE_HIERARCHY
    )
    result_df = ls_phase1_gen_map.dedupe_overlap_higher_iam_grant(
        result_df, TEST_ROLE_HIERARCHY
    )
    assert_frame_equal(expected_df, result_df)


def test_dedupe_identical_and_ds_inheritance():
    """First mark identical IAM grants for the same resource, then mark permissions inherited from the dataset-level."""
    input_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "editor"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "viewer"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl1", "editor"],
            ["group1@example.com", "proj1", "ds1", "tbl2", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl2", "admin"],
            ["group1@example.com", "proj1", "ds2", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "editor"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "viewer"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "editor"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "viewer"],
            ["group1@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group1@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds3", consts.ALL, "admin"],
            ["group2@example.com", "proj2", "ds3", consts.ALL, "admin"],
            ["group2@example.com", "proj2", "ds3", "tbl3", "admin"],
        ],
        columns=TEST_COLUMNS,
    )
    expected_df = pd.DataFrame(
        [
            ["group1@example.com", "proj1", "ds1", consts.ALL, "editor"],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.INHERITED_FROM_DS],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds1", "tbl1", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl1", consts.INHERITED_FROM_DS],
            ["group1@example.com", "proj1", "ds1", "tbl2", "admin"],
            ["group1@example.com", "proj1", "ds1", "tbl2", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds2", consts.ALL, "viewer"],
            ["group1@example.com", "proj1", "ds2", "tbl2", "editor"],
            ["group1@example.com", "proj1", "ds2", "tbl2", consts.INHERITED_FROM_DS],
            ["group1@example.com", "proj1", "ds2", "tbl2", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj1", "ds2", "tbl2", consts.DUPLICATE_GRANT],
            ["group1@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group1@example.com", "proj2", "ds2", "tbl2", consts.DUPLICATE_GRANT],
            ["group2@example.com", "proj2", "ds2", "tbl2", "viewer"],
            ["group2@example.com", "proj2", "ds2", "tbl2", consts.DUPLICATE_GRANT],
            ["group2@example.com", "proj2", "ds3", consts.ALL, "admin"],
            ["group2@example.com", "proj2", "ds3", consts.ALL, consts.DUPLICATE_GRANT],
            ["group2@example.com", "proj2", "ds3", "tbl3", consts.INHERITED_FROM_DS],
        ],
        columns=TEST_COLUMNS,
    )

    result_df = input_df.copy()
    result_df = ls_phase1_gen_map.dedupe_identical_iam_role_grants(result_df)
    result_df = ls_phase1_gen_map.dedupe_dataset_iam_inheritance(
        result_df, TEST_ROLE_HIERARCHY
    )
    assert_frame_equal(expected_df, result_df)


def test_iam_hierarchy():
    # raise NotImplementedError()
    table_series = pd.Series(["viewer", "editor", "admin"])
    ds_iam_role = "editor"
    expected_series = pd.Series([True, True, False])

    result_series = ls_phase1_gen_map.is_iam_role_as_or_more_permissive(
        table_series, ds_iam_role, TEST_ROLE_HIERARCHY
    )
    assert_series_equal(expected_series, result_series)
