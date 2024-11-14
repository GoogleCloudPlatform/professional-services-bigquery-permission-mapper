resource "google_bigquery_dataset_iam_member" "dataset_iam_member" {
  for_each = { for member in var.members : "${member.role}-${member.member}" => member }

  project = var.project_id
  dataset_id = var.dataset_id
  role       = each.value.role
  member     = each.value.member
}