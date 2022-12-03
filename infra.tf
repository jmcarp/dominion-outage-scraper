variable "project" {
  default = "cvilledata"
}

terraform {
  backend "gcs" {
    bucket = "cvilledata-terraform-state"
    prefix = "dominion-outage-scraper"
  }
  required_version = "~> 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.44"
    }
  }
}

provider "google" {
  region  = "us-central1"
  project = var.project
}

resource "google_pubsub_topic" "scrape" {
  project = var.project
  name    = "scrape"
}

resource "google_cloud_scheduler_job" "scrape" {
  project = var.project
  name    = "scrape"

  // Scrape every 15 minutes, starting at 5 past the hour. Data update every 15 minutes, but we
  // don't want to scrape in the middle of a data upload to make sure we don't miss files that
  // haven't uploaded yet. It's not clear that this scenario can happen, but it's hard to prove
  // that it can't.
  schedule = "5-59/15 * * * *"

  pubsub_target {
    topic_name = google_pubsub_topic.scrape.id
    // Must set either data or at least one attribute
    attributes = {
      dummy = "dummy"
    }
  }
}

data "archive_file" "scrape" {
  type        = "zip"
  output_path = "${path.module}/scrape.zip"

  source {
    content  = file("${path.module}/requirements.txt")
    filename = "requirements.txt"
  }

  source {
    content  = file("${path.module}/main.py")
    filename = "main.py"
  }
}

resource "google_storage_bucket" "functions" {
  project  = var.project
  location = "US"
  name     = "dominion-scraper-functions"
}

resource "google_storage_bucket_object" "scrape" {
  name   = "${data.archive_file.scrape.output_sha}.zip"
  bucket = google_storage_bucket.functions.name
  source = data.archive_file.scrape.output_path
}

resource "google_cloudfunctions_function" "scrape" {
  project = var.project
  name    = "scrape"
  runtime = "python310"

  source_archive_bucket = google_storage_bucket.functions.name
  source_archive_object = google_storage_bucket_object.scrape.name
  entry_point           = "entrypoint"
  timeout               = 540

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.scrape.name
  }

  depends_on = [google_storage_bucket_object.scrape]
}
