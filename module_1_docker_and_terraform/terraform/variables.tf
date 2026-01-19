variable "project" {
    description = "The GCP project ID"
    type        = string
    default     = "triple-acre-484506-h5"
  
}

variable "region" {
    description = "The GCP region"
    type        = string
    default     = "us-central1"
  
}

variable "location" {
    description = "The location for GCP resources"
    type        = string
    default     = "US"
  
}

variable "bq_dataset_name" {
    description = "The name of the BigQuery dataset to create"
    type        = string
    default     = "zoomcamp_dataset"  
}

variable "gcs_bucket_name" {
    description = "The name of the GCS bucket to create"
    type        = string
    default     = "triple-acre-484506-h5-no-age-enabled-bucket"
  
}


variable "gcs_storage_class" {
    description = "The storage class of the GCS bucket"
    type        = string
    default     = "STANDARD"
}