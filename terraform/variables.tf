variable "credentials" {
  description = "my credentials to access GCP from TERRFORM"
  default     = "~/keys/afcon_project/my-creds.json"

}

variable "project" {
  description = "my project name"
  default     = "data-engineering-afcon-2023"


}

variable "region" {
  description = "region of project on GCP"
  default     = "us-central1"

}

variable "google_storage_bucket_name" {
  description = "my GCP bucket name"
  default     = "afcon_datalake"

}

variable "resources_location" {
  description = "location of deployed resources on GCP"
  default     = "US"

}

variable "google_bigquery_dataset_name" {
  description = "bigquery dataset name"
  default     = "afcon_events"

}