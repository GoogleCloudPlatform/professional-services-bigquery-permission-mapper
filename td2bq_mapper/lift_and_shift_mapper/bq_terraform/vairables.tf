variable "datasets" {
  type = list(object({
    project_id = string
    dataset_id = string
    members    = list(object({
      role   = string
      member = string
    }))
  }))
  description = "List of datasets with their project IDs and IAM members"
}

variable "tables" {
  type = list(object({
    project_id = string
    dataset_id = string
    table_id   = string
    members    = list(object({
      role   = string
      member = string
    }))
  }))
  description = "List of tables with their project IDs, dataset IDs, and IAM members"
}