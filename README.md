# OTRS Prometheus Exporter

The OTRS Prometheus Exporter has been created to facilitate monitoring of certain aspects of the applications' functionality, as well as some usage statistics.

## Planned features

* systemd - Integration
  * Monitor if otrs-webserver & otrs-daemon systemd services are up and running and fully functional
  * Run this exporter via systemd
* Monitor if e-mail fetching is working as intended
  * based on log-file
* Export ticket / queue statistics from MySQL DB -> potentially replacing Telegraf / InfluxDB completely
