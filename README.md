# Teradata to BigQuery Permission Mapper
The mapper automatically translates Teradata permissions to BigQuery IAM. The tool translates Teradata roles and ACL to GCP user groups, BigQuery custom roles, and IAM permissions. It outputs a translation CSV report, JSON files for gcloud CLI, and Terraforms to create BigQuery custom roles with corresponding IAM permissions and assign those roles to GCP user groups and BigQuery resources.


## Requirements
To setup your environment to run the script you will need to install the
python dependencies via pip.
```
pip install -r requirements.txt
```

## Run the Mapper
The mapper tool is a directory of scripts which relies on the accuracy of input files provided by the end user. The scripts and reports generated will be available at the local directory for the user to review and use. The steps to generate the BigQuery IAM permission as JSON files or Terraform scripts are:
1. Unpack the BigQuery ACL Mapper tool and set up a directory for the tool to dump files to
2. Execute the provided SQL query to retrieve and export Teradata permissions from Teradata
3. Run the mapper tool and verify the resources and permissions that need to be migrated
4. Generate and review the JSON files or Terraform scripts, and a CSV report containing the mapping summary

```
$ python td2bq.py --help
```

See the detailed [user guide PDF](docs/bq-permission-mapper-user-guide.pdf) for more information. 

## Disclaimer

This is not an officially supported Google product. Nonetheless, it is under active development - please feel free to open bugs or feature requests, or contribute directly (see [`CONTRIBUTING.md`](docs/CONTRIBUTING.md) for details).
