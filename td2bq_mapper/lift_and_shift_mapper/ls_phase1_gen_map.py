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

"""Runs Phase 1 of the lift-and-shift mapper to generate and output the planned IAM mapping."""

import datetime
import logging
import os

import pandas as pd

try:
    import td2bq_util
    from lift_and_shift_mapper import consts
except ImportError:
    import sys

    sys.path.append(sys.path[0] + "/..")
    try:
        import td2bq_util
        from lift_and_shift_mapper import consts
    except ImportError:
        # for pytest test_phase1.py to find modules
        import td2bq_mapper.td2bq_util
        from td2bq_mapper.lift_and_shift_mapper import consts


def df_contains_nulls(df: pd.DataFrame) -> bool:
    """Check that all columns are populated, except for IAMRole which should be blank."""
    cols_with_na = df.drop("IAMRole", axis=1).isna().any()
    return cols_with_na.any()


def missing_arc_mappings(mapping_df: pd.DataFrame, arc_map: dict) -> list:
    """Return list of any access rights present in the df that do not have a mapping defined."""
    missing_rights = []
    for access_right in mapping_df["AccessRight"].unique():
        if access_right not in arc_map.keys():
            missing_rights.append(access_right)
    return missing_rights


def dedupe_dataset_iam_inheritance(
    df: pd.DataFrame, role_hierarchy: dict
) -> pd.DataFrame:
    """If table-level permissions are already granted at the dataset-level, mark the table-level ones as so."""
    for i in range(len(df)):
        if df.loc[i, consts.BQ_TABLE] == consts.ALL:
            mask = (
                (df[consts.GCP_GROUP] == df.loc[i, consts.GCP_GROUP])
                & (df[consts.GCP_PROJECT] == df.loc[i, consts.GCP_PROJECT])
                & (df[consts.BQ_DATASET] == df.loc[i, consts.BQ_DATASET])
                & (df[consts.BQ_TABLE] != consts.ALL)  # don't overwrite itself
                & (
                    is_iam_role_as_or_more_permissive(
                        df[consts.IAM_ROLE], df.loc[i, consts.IAM_ROLE], role_hierarchy
                    )
                )
            )
            df.loc[mask, consts.IAM_ROLE] = consts.INHERITED_FROM_DS
    return df


def dedupe_overlap_higher_iam_grant(
    df: pd.DataFrame, role_hierarchy: dict
) -> pd.DataFrame:
    """Mark IAM roles that are obsolete due to more permissive ones present. Applies to both tables and datasets."""
    for i in range(len(df)):
        mask = (
            (df[consts.GCP_GROUP] == df.loc[i, consts.GCP_GROUP])
            & (df[consts.GCP_PROJECT] == df.loc[i, consts.GCP_PROJECT])
            & (df[consts.BQ_DATASET] == df.loc[i, consts.BQ_DATASET])
            & (df[consts.BQ_TABLE] == df.loc[i, consts.BQ_TABLE])
            & (df[consts.IAM_ROLE] != df.loc[i, consts.IAM_ROLE])
            & (
                is_iam_role_as_or_more_permissive(
                    df[consts.IAM_ROLE], df.loc[i, consts.IAM_ROLE], role_hierarchy
                )
            )
        )
        df.loc[mask, consts.IAM_ROLE] = consts.OVERLAP_GRANT
    return df


def role_comparison(table_iam_role, dataset_iam_role, role_hierarchy):
    if table_iam_role.startswith("MAPPER:") or (table_iam_role == "NOT_APPLICABLE"):
        return False
    else:
        return role_hierarchy[table_iam_role] >= role_hierarchy[dataset_iam_role]


def is_iam_role_as_or_more_permissive(
    table_iam_role_series: pd.Series, dataset_iam_role: str, role_hierarchy: dict
) -> pd.Series:
    """Check whether the dataset-level role is equal to or more permissive than the table-level one."""
    return table_iam_role_series.apply(
        role_comparison, args=(dataset_iam_role, role_hierarchy)
    )


def dedupe_identical_iam_role_grants(df: pd.DataFrame) -> pd.DataFrame:
    """If two rows in the same binding will grant the same role/permissions, keep one and mark the others as duplicates."""
    dedupe_table_mask = df.groupby(
        [
            consts.GCP_GROUP,
            consts.GCP_PROJECT,
            consts.BQ_DATASET,
            consts.BQ_TABLE,
            consts.IAM_ROLE,
        ]
    ).cumcount().ne(0) | (
        df[consts.IAM_ROLE].apply(lambda iam_role: iam_role.startswith("MAPPER:"))
    )
    df.loc[dedupe_table_mask, consts.IAM_ROLE] = consts.DUPLICATE_GRANT
    return df


def generate_mapping():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("td2bq_lift_and_shift")

    # Read the input CSV
    logger.info("Reading and validating provided CSV access map input...")
    mapping_csv_path = os.path.join(
        td2bq_util.get_root_dir(),
        f"./lift_and_shift_mapper/data/{consts.ACCESS_MAP_INPUT_CSV}",
    )
    mapping_df = pd.read_csv(mapping_csv_path)

    # Validate that required columns are present and named as expected in the CSV.
    if set(consts.COLUMNS).issubset(mapping_df.columns):
        raise ValueError(
            f"Input CSV column names do not match expected names defined in consts.py.\nInput columns:{mapping_df.columns.values}\nExpected columns:{consts.COLUMNS}"
        )

    # Validate that there aren't any blank fields that are required.
    if df_contains_nulls(mapping_df):
        raise TypeError(
            "Input CSV contains null fields where values are expected. Please check that all columns are populated."
        )

    # Read the map file defining TD ARCs to GCP IAM roles.
    # Unlike the standard mapper's ARC json, this maps to an IAM role directly rather than to granular permissions.
    logger.info("Reading and validating provided ARC map...")
    map_file_path = os.path.join(
        td2bq_util.get_root_dir(),
        f"./lift_and_shift_mapper/data/{consts.PREDEFINED_ARC_MAP_JSON}",
    )
    arc_map = td2bq_util.read_json_file(map_file_path)
    if not arc_map:
        raise IOError("Error reading ARC Map JSON file, or it may be empty.")

    # Validate that all present access rights are addressed in the arc_map.
    missing_rights = missing_arc_mappings(mapping_df, arc_map)
    if missing_rights:
        raise Exception(
            f"The following access rights present in the input CSV do not have a mapping defined in the arc_map.json. Please update the arc_map:\n{missing_rights}"
        )

    # Populate a new column with the proposed roles.
    logger.info("Populating IAM roles...")
    mapping_df[consts.IAM_ROLE] = mapping_df[consts.TD_ACCESS_RIGHT].apply(
        lambda arc: arc_map[arc]["iam_role"]
    )

    # Deduplicate roles based on provided hierarchy/permissiveness.
    logger.info("Deduplicating identical IAM roles...")
    mapping_df = dedupe_identical_iam_role_grants(mapping_df)

    logger.info("Marking IAM roles inherited from dataset-level grants...")
    mapping_df = dedupe_dataset_iam_inheritance(mapping_df, consts.ROLE_HIERARCHY)

    logger.info("Marking overlapping less permissive IAM roles per table/dataset...")
    mapping_df = dedupe_overlap_higher_iam_grant(mapping_df, consts.ROLE_HIERARCHY)

    # Write to new CSV.
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H.%M.%S")
    output_path = os.path.join(
        os.getcwd(),
        f"td2bq_mapper/lift_and_shift_mapper/data/phase1_out_{now}.csv",
    )
    logger.info(f"Writing output to {output_path}")
    mapping_df.to_csv(output_path)
    logger.info("Mapper Phase 1 completed.")


if __name__ == "__main__":
    generate_mapping()
