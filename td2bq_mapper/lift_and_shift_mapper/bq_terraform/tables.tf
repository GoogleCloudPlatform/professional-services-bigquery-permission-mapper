module "table_iam" {
  source = "./modules/bigquery-table-iam-member"

  for_each = { for table in var.tables : "${table.project_id}-${table.dataset_id}-${table.table_id}" => table }

  project_id = each.value.project_id
  dataset_id = each.value.dataset_id
  table_id   = each.value.table_id
  members    = each.value.members
}