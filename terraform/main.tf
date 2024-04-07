terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "gcp_bucket" {
  name          = var.google_storage_bucket_name
  location      = var.resources_location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "bigquery_dataset" {
  dataset_id                 = var.google_bigquery_dataset_name
  project                    = var.project
  location                   = var.resources_location
  delete_contents_on_destroy = true
}