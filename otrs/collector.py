import logging
from typing import List, Dict

from prometheus_client.core import GaugeMetricFamily, InfoMetricFamily, StateSetMetricFamily
from pygtail import Pygtail
import subprocess
import re
import settings

log: Pygtail = Pygtail(settings.LOGWATCH, offset_file="/dev/shm/logfile.offset")
LOG = logging.getLogger(__name__)


def call_otrs_cli(cli_endpoint: str) -> str:
    return subprocess.run(["/opt/otrs/bin/otrs.Console.pl", cli_endpoint],
                          stdout=subprocess.PIPE).stdout.decode("utf-8")


class OtrsConnector:

    def collect(self):
        yield self._metric_mail_error_count()
        yield self._metric_mail_queue_empty()
        yield self._metric_failing_crons()
        yield self._metric_config_valid()
        yield self._metric_db_status_ok()
        yield self._metric_db_additional_stats()
        yield self._metric_daemon_summary()
        yield self._metric_elastic_status_ok()
        yield self._metric_elastic_cluster_status()
        yield self._metric_elastic_all_nodes_status()
        yield self._metric_elastic_overall_node_status()
        yield self._metric_elastic_index_states()

    def _metric_mail_error_count(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_mail_error", "Determine via OTRS logs whether there are issues with E-Mail")
        LOG.debug("Added mail error count metric")
        metric.add_metric([], get_mail_fetcher_errors())
        return metric

    def _metric_config_valid(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_config_valid", "Is the OTRS Config valid or not?")
        LOG.debug("Added OTRS valid config metric")
        metric.add_metric([], is_config_valid())
        return metric

    def _metric_daemon_summary(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_daemon_summary", "The OTRS Daemon Success Rate")
        LOG.debug("Added OTRS daemon success rate metric")
        metric.add_metric([], get_job_success_rate())
        return metric

    def _metric_failing_crons(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_daemon_cron_jobs",
                                  "This tells us about all failing OTRS Daemon Cron Jobs")
        LOG.debug("Added failing cron job metric")
        metric.add_metric([], get_failing_crons())
        return metric

    def _metric_db_status_ok(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_db_status_ok",
                                   "This tells us about all failing OTRS Daemon Cron Jobs")
        LOG.debug("Added Db status ok metric")
        metric.add_metric([], get_db_status())
        return metric

    def _metric_db_additional_stats(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_daemon_additional_db_stats",
                                  "This tells us about all additional DB stats gathered by OTRS")
        LOG.debug("Added additional db stats metric")
        metric.add_metric([], get_additional_db_stats())
        return metric

    def _metric_elastic_status_ok(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_elastic_status_ok",
                                   "This tells us about all failing OTRS Daemon Cron Jobs")
        LOG.debug("Added elastic status ok metric")
        metric.add_metric([], get_elastic_status())
        return metric

    def _metric_elastic_cluster_status(self) -> StateSetMetricFamily:
        metric = StateSetMetricFamily("otrs_elastic_cluster_status",
                                      "This tells us about all ElasticSearch Cluster Health")
        LOG.debug("Added elastic cluster status metric")
        metric.add_metric([], get_elastic_overall_cluster_status())
        return metric

    def _metric_elastic_overall_node_status(self) -> StateSetMetricFamily:
        metric = StateSetMetricFamily("otrs_elastic_node_status",
                                      "This tells us about the overall ElasticSearch Node Health")
        LOG.debug("Added elastic node status metric")
        metric.add_metric([], get_elastic_overall_nodes_status())
        return metric

    def _metric_elastic_all_nodes_status(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_elastic_failed_nodes",
                                  "This tells us about all ElasticSearch nodes")
        LOG.debug("Added elastic node status metric")
        metric.add_metric([], get_elastic_all_nodes_states())
        return metric

    def _metric_elastic_index_states(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_elastic_index_states",
                                  "This tells us about all ElasticSearch nodes")
        LOG.debug("Added elastic index status metric")
        metric.add_metric([], get_elastic_index_states())
        return metric

    def _metric_mail_queue_empty(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_elastic_status_ok",
                                   "This tells us about all failing OTRS Daemon Cron Jobs")
        LOG.debug("Added mail queue empty metric")
        metric.add_metric([], get_mail_queue_empty())
        return metric


def get_mail_queue_empty() -> int:
    otrs_cli_out: str = subprocess.run(["/opt/otrs/bin/otrs.Console.pl", "Maint::Email::MailQueue", "--list"],
                                       stdout=subprocess.PIPE).stdout.decode("utf-8")
    if "Mail queue is empty." in otrs_cli_out:
        return 1
    else:
        return 0


def get_elastic_index_states() -> Dict:
    otrs_cli_out: str = subprocess.run(["/opt/otrs/bin/otrs.Console.pl",
                                        "Maint::DocumentSearch::IndexManagement", "--index-status", "all"],
                                       stdout=subprocess.PIPE).stdout.decode("utf-8")
    indices: List[str] = re.compile(r"^\s+\|\s+(\w+)\s+\|\s+(\d+)\s+\|\s+(\d+)\s+\|.*$", re.MULTILINE).findall(
        otrs_cli_out)
    indices_indexed = {re.sub(r'(?<!^)(?=[A-Z])', '_', index[0]).lower() + "_indexed": index[2] for index in indices}
    indices_avail = {re.sub(r'(?<!^)(?=[A-Z])', '_', index[0]).lower() + "_avail": index[1] for index in indices}
    indices_dict = indices_indexed
    indices_dict.update(indices_avail)
    return indices_dict


def get_elastic_all_nodes_states() -> Dict:
    otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
    nodes: List[str] = re.compile(r"^\s+\|\s+Node\s+\|\s+(\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
    states: List[str] = re.compile(r"^\s+\|\s+Status\s+\|\s+(\w+\W\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
    node_to_state = {n: s for n, s in zip(nodes, states)}
    return node_to_state


def get_elastic_overall_nodes_status():
    otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
    states: List[str] = re.compile(r"^\s+\|\s+Status\s+\|\s+(\w+\W\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
    unique_status = set(states)
    if "On-line" not in unique_status:
        return {"Red": None}
    # If one node goes bad, the overall node state should turn bad
    elif len(unique_status) > 1:
        return {"Yellow": False}
    else:
        return {"Green": True}


def get_elastic_overall_cluster_status():
    otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
    matching: List[str] = re.compile(r"^\s+\|\s+Status\s+\|\s+(\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
    state_to_bool = {
        "Red": None,
        "Yellow": False,
        "Green": True
    }
    result = {}
    for match in matching:
        result[match] = state_to_bool[match]
    return result


def get_elastic_status():
    otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
    if "Connection successful." in otrs_cli_out:
        return 1
    else:
        return 0


def get_additional_db_stats():
    otrs_cli_out: str = call_otrs_cli("Maint::Database::Check")
    matching: List[str] = re.compile(r"^(\w[\w\s]+)\: (.*)$", re.MULTILINE).findall(otrs_cli_out)
    return {match[0].lower().replace(" ", "_"):
            match[1].strip(" ").strip("(").strip(")").replace(" (", " - ")
            for match in matching}


def get_db_status():
    otrs_cli_out: str = call_otrs_cli("Maint::Database::Check")
    if "Connection successful." in otrs_cli_out:
        return 1
    else:
        return 0


def get_failing_crons():
    otrs_cli_out: str = call_otrs_cli("Maint::Daemon::Summary")
    matching: List[str] = re.compile(r"^\s*\|\s*([A-Za-z0-9]+)\s*\|\s*[\s\d:\-]*\|\s*Fail", re.MULTILINE).findall(
        otrs_cli_out)
    return {match: "failed" for match in matching}


def get_job_success_rate():
    otrs_cli_out: str = call_otrs_cli("Maint::Daemon::Summary")
    daemon_success = otrs_cli_out.count("Success")
    daemon_total = daemon_success + otrs_cli_out.count("Fail")
    return daemon_success / daemon_total


def is_config_valid():
    otrs_cli_out: str = call_otrs_cli("Admin::Config::ListInvalid")
    if "All settings are valid." in otrs_cli_out:
        return 1
    else:
        return 0


def get_mail_fetcher_errors():
    error_count = 0
    for line in log:
        if any(s in line for s in ["Got no email",
                                   "S/MIME",
                                   "Could not re-process email",
                                   "PostMaster"]):
            error_count += 1
    return error_count
