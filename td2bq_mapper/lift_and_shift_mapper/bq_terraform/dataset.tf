module "dataset_iam" {
  source = "./modules/bigquery-dataset-iam-member"

  for_each = { for dataset in var.datasets : dataset.dataset_id => dataset }

  project_id = each.value.project_id
  dataset_id = each.value.dataset_id
  members    = each.value.members
}