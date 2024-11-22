# Teradata to BigQuery Permission Mapper
The TD2BQ Permission Mapper translate Teradata permissions to BigQuery IAM. Teradata roles are mapped GCP groups and IAM roles. It outputs Terraform to apply these mappings, along with a a CSV mapping report.

There are two variants of the permission mapper:

### 1. Lift-and-Shift Mapper
- Use this Mapper version if you want to retain the same Teradata roles in BigQuery, as well as if you prefer to use predefined IAM roles.
- This version retains all Teradata roles in the GCP translation.
- Teradata ACLs are mapped to IAM roles according to the user-configurable JSON map.
- By default, BigQuery predefined roles are provided in the mapping. This can be changed. A hierarchy can also be defined so that groups are not granted less permissive roles if they already have a more permission one (eg. a group would be granted just Data Editor instead of both Data Editor and Data Viewer).
- The tool will deduplicate role grants where possible. For example, if a group is granted BigQuery Data Viewer on both a schema/dataset, as well as a table within that schema, it will only be granted at the schema-level since it will be inherited.
- Refer to the [Lift-and-Shift Mapper ReadMe](td2bq_mapper/lift_and_shift_mapper//README.md) for more details.


### 2. Custom Role Mapper
- Use this version if you want to create a fresh set of roles and groups rather than migrating your existing Teradata roles. Also use this if you need GCP custom roles created based on the permissions present in Teradata.
- The tool translates Teradata roles and ACL to GCP user groups, BigQuery custom roles, and IAM permissions.
- It outputs a translation CSV report, JSON files for gcloud CLI, and Terraform to create BigQuery custom roles with corresponding IAM permissions and assign those roles to GCP user groups and BigQuery resources.
- Refer to the below steps for more info on the Custom Role mapper, including detailed user guide.


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
