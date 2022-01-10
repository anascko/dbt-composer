resource "random_id" "random_suffix" {
  byte_length = 2
}

module "composer" {
  source  = "terraform-google-modules/composer/google//modules/create_environment"
  version = "2.2.0"

  composer_env_name       = "${var.composer_name}-${random_id.random_suffix.hex}"
  project_id = var.project_id
  region     = var.region


  node_count = 3

  zone                     = var.zone
  network                  = google_compute_network.main.name
  subnetwork               = google_compute_subnetwork.main.name
  composer_service_account = google_service_account.sa_composer.email
  machine_type             = var.machine_type
  pod_ip_allocation_range_name     = google_compute_subnetwork.main.secondary_ip_range[0].range_name
  service_ip_allocation_range_name = google_compute_subnetwork.main.secondary_ip_range[1].range_name
  #disk_size = var.disk_size
}
