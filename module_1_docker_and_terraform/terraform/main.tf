terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = "triple-acre-484506-h5"
  region  = "us-central1"
}


resource "google_storage_bucket" "demo-bucket" {
  name          = "triple-acre-484506-h5-no-age-enabled-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      days_since_noncurrent_time = 85
      send_age_if_zero = false
    }
  }
}