variable "project_id" {
  type = string
  description = "The ID of the Google Cloud project"
}

variable "dataset_id" {
  type = string
  description = "The ID of the BigQuery dataset"
}

variable "table_id" {
  type = string
  description = "The ID of the BigQuery table"
}

variable "members" {
  type = list(object({
    role   = string
    member = string
  }))
  description = "List of IAM members and their roles"
}