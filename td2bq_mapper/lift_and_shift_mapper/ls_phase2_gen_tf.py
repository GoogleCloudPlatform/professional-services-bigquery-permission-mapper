# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

import argparse

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import csv
import logging
import os
import sys

from jinja2 import Environment, FileSystemLoader

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


def generate_terraform_output(
    phase1_csv=None, dataset_access_template_name=None, table_access_template_name=None
):
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("td2bq_lift_and_shift")

    if phase1_csv is None:
        phase1_csv = os.path.join(
            td2bq_util.get_root_dir(),
            "lift_and_shift_mapper/data/",
            f"{consts.ACCESS_MAP_PHASE1_OUTPUT_CSV}",
        )

    logger.info("Loading the template files...")
    data_path = os.path.join(
        td2bq_util.get_root_dir(),
        "lift_and_shift_mapper/data/",
    )
    terraform_path = os.path.join(
        td2bq_util.get_root_dir(),
        "lift_and_shift_mapper/bq_terraform/"                          )
    file_loader = FileSystemLoader(
        searchpath=data_path
    )  # Assume templates are in the same directory
    env = Environment(loader=file_loader)
    if dataset_access_template_name is None:
        dataset_access_template_name = consts.DATASET_ACCESS_TEMPLATE
    if table_access_template_name is None:
        table_access_template_name = consts.TABLE_ACCESS_TEMPLATE

    dataset_access_template = env.get_template(dataset_access_template_name)
    table_access_template =  env.get_template(table_access_template_name)

    table_access = {}
    # Read the input CSV
    logger.info("Reading the output of Phase1 mapper...")
    datasets = {}
    with open(phase1_csv, "r") as csv_file:
        reader = csv.DictReader(csv_file)
        for i, row in enumerate(reader):
            # Reading only rows that has a bigquery role assigned.This eliminates audit entries.
            if row["IAMRole"].startswith("roles/"):
                if row["BQTableName"] == "All":  # Filtering out dataset access rows.
                    if row["BQDatasetName"] not in datasets.keys():
                        users = [row]
                        datasets[row["BQDatasetName"]] = {
                            "BQDatasetName": row["BQDatasetName"],"GCPProjectId":row["GCPProjectId"],
                            "users": users,
                        }
                    else:
                        datasets[row["BQDatasetName"]]["users"].append(row)
                else:
                     if row["BQDatasetName"] not in table_access.keys():
                        users = [row]
                        table_access[row["TableName"]] = {
                            "TableName":row["TableName"],"BQDatasetName": row["BQDatasetName"],"GCPProjectId":row["GCPProjectId"],
                            "users": users,
                        }
                     else:
                        table_access[row["TableName"]]["users"].append(row)

    logger.info("Rendering the templates with actual values...")
    dataset_access_data = dataset_access_template.render(
        dataset_access=list(datasets.values())
    )
    table_access_data = table_access_template.render(table_accesses=list(table_access.values()))
    # Generating output file path variables.
    dataset_access_output_file = terraform_path + "dataset.tfvars"
    table_access_output_file = terraform_path + "tables.tfvars"

    # writing the template files to data path.
    logger.info("creating the output terraform files in the data path...")
    with open(dataset_access_output_file, "w") as outfile:
        outfile.write(dataset_access_data)

    with open(table_access_output_file, "w") as outfile:
        outfile.write(table_access_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate Terraform output from CSV and template"
    )
    parser.add_argument(
        "--phase1_csv",
        help="Path to the CSV Phase1 output file to be used to generate Terraform",
    )
    parser.add_argument(
        "--dataset_template", help="Path to the Terraform template file"
    )
    parser.add_argument("--table_template", help="Path to the Terraform template file")
    args = parser.parse_args()

    generate_terraform_output(
        phase1_csv=args.phase1_csv,
        dataset_access_template_name=args.dataset_template,
        table_access_template_name=args.table_template,
    )
