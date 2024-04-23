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
import csv
import logging
import os
import sys

from jinja2 import FileSystemLoader, Environment

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



def generate_terraform_output():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("td2bq_lift_and_shift")

    data_path = os.path.join(
        td2bq_util.get_root_dir(),
        "lift_and_shift_mapper/data/",
        )
    phas1_output_map_file_path = os.path.join(td2bq_util.get_root_dir(),
                                                     "lift_and_shift_mapper/data/",f"{consts.ACCESS_MAP_PHASE1_OUTPUT_CSV}",
    )

    logger.info("Loading the template files...")
    file_loader = FileSystemLoader(searchpath=data_path)  # Assume templates are in the same directory
    env = Environment(loader=file_loader)
    dataset_access_template = env.get_template(consts.DATASET_ACCESS_TEMPLATE)
    table_access_template = env.get_template(consts.TABLE_ACCESS_TEMPLATE)

    dataset_access = []
    table_access = []
    # Read the input CSV
    logger.info("Reading the output of Phase1 mapper...")
    with open(phas1_output_map_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            # reading only rows that has a bigquery role assigned.This eliminates audit entires.

            if row['IAMRole'].startswith('roles/'):
                if row['BQTableName'] == 'All':  # Filtering out dataset access rows.
                    dataset_access.append(row)
                else:
                    table_access.append(row)

    logger.info("Rendering the templates with actual values...")
    dataset_access_data = dataset_access_template.render(dataset_access=dataset_access)
    table_access_data = table_access_template.render(table_accesses=table_access)
    # Generating output file path variables.
    dataset_access_output_file = data_path + "dataset_access_locals.tf"
    table_access_output_file = data_path + "table_access_locals.tf"

    # writing the template files to data path.
    logger.info("creating the output terraform files in the data path...")
    with open(dataset_access_output_file, 'w') as outfile:
        outfile.write(dataset_access_data)

    with open(table_access_output_file, 'w') as outfile:
        outfile.write(table_access_data)


if __name__ == "__main__":
    # calling the Generate terraform function.
    generate_terraform_output()
