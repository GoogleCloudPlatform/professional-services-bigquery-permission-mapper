# Lift-and-Shift Mapper
## Overview
This is an alternate implementation of the Teradata-to-BigQuery Permission Mapper. It aims to maintain the same existing Teradata roles when migrating to BigQuery. It also uses an IAM-role-based approach, rather than creating new custom roles. This sacrifices granularity for ease of management and speed of migration.

The tool runs in two phases:
- Phase 1: Read the CSV input, populate suggested IAM roles, and mark which accesses will be deduplicated. Write results to a new CSV.
- Phase 2: Generate Terraform based on Phase 1's output.

## Steps to use
1. Generate the access map input CSV. This should include the columns mentioned in `consts.COLUMNS`. The `IAM_ROLE` column should be empty.
2. Update `predefined_arc_map.json` to provide a mapping to BQ IAM roles for all TD access rights that appear in the access map input CSV.
3. Update the role priority list defined in `consts.ROLE_HIERARCHY`. Lower values mean higher priority. If a planned dataset-level IAM binding will already provide a higher priority role, the table-level binding is removed.
4. Update the `ACCESS_MAP_INPUT_CSV` and `PREDEFINED_ARC_MAP_JSON` file names in `consts.py` if needed. Place these files in `td2bq_mapper/lift_and_shift_mapper/data/`.
5. Run Phase 1 of the tool - from the top directory, `python3 ./td2bq_mapper/lift_and_shift_mapper/ls_phase1_gen_map.py`. By default, the output is also written to `td2bq_mapper/lift_and_shift_mapper/data/`. Verify the output looks good.
6. Run Phase 2 of the tool - `python3 ./td2bq_mapper/lift_and_shift_mapper/ls_phase2_gen_tf.py`. Verify the Terraform looks good.
7. Apply the Terraform to your GCP project (BigQuery table schemas must already exist).

## Tests
Tests are found in `td2bq_mapper/lift_and_shift_mapper/tests/`. They can be run with `pytest td2bq_mapper/lift_and_shift_mapper/tests/test_phase1.py`. The tests can be helpful in understanding the expected input vs output (in particular, see `test_inheritance_and_table_role_dedupe()`).