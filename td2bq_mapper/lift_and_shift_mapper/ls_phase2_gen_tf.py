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
import argparse
import os

from jinja2 import FileSystemLoader, Environment


def generate_terraform_output(access_list_csv,dataset_access_template_file,table_access_template_file):


    data_path = os.path.join(
        os.getcwd(),
        f"data/",
    )
    print(f"data path : {data_path}")
    file_loader = FileSystemLoader(searchpath=data_path)  # Assume templates are in the same directory
    env = Environment(loader=file_loader)
    dataset_access_template = env.get_template(dataset_access_template_file)
    table_access_template = env.get_template(table_access_template_file)


    dataset_access = []
    table_access =[]
    with open(data_path+access_list_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            if row['BQTableName'] == 'All':
                dataset_access.append(row)
            else:
                table_access.append(row)

    dataset_access_data = dataset_access_template.render(dataset_access=dataset_access)
    table_access_data = table_access_template.render(table_accesses=table_access)

    dataset_access_output_file = data_path + "dataset_access_locals.tf"
    table_access_output_file = data_path + "table_access_locals.tf"
    with open(dataset_access_output_file, 'w') as outfile:
        outfile.write(dataset_access_data)

    with open(table_access_output_file, 'w') as outfile:
        outfile.write(table_access_data)


if __name__ == "__main__":
    '''parser = argparse.ArgumentParser(description="Generate Terraform output from CSV and template")
    parser.add_argument("csv_file", help="Path to the CSV input file")
    parser.add_argument("template_file", help="Path to the Terraform template file")
    parser.add_argument("output_file", help="Path to the generated Terraform output file")
    args = parser.parse_args()'''
    generate_terraform_output('test-fmcc_input-phase1_output.csv','dataset_access_template.jinja','table_access_teamplate.jinja')