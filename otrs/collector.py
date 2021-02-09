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
        metric = GaugeMetricFamily("otrs_mail_error",
                                   "Determine via OTRS logs whether there are issues with E-Mail")
        LOG.debug("Added mail error count metric")
        metric.add_metric([], get_mail_fetcher_errors())
        return metric

    def _metric_config_valid(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_config_valid",
                                   "Return 1 if OTRS config is valid and 0 if not")
        LOG.debug("Added OTRS valid config metric")
        metric.add_metric([], is_config_valid())
        return metric

    def _metric_daemon_summary(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_daemon_summary",
                                   "The OTRS Daemon Success Rate - How many tasks monitored in the Daemon command "
                                   "fail.")
        LOG.debug("Added OTRS daemon success rate metric")
        metric.add_metric([], get_job_success_rate())
        return metric

    def _metric_failing_crons(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_daemon_cron_jobs",
                                  "List all failing OTRS Daemon Cron Jobs")
        LOG.debug("Added failing cron job metric")
        metric.add_metric([], get_failing_crons())
        return metric

    def _metric_db_status_ok(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_db_status_ok",
                                   "Return 1 if connection successful and 0 if not")
        LOG.debug("Added Db status ok metric")
        metric.add_metric([], get_db_status())
        return metric

    def _metric_db_additional_stats(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_daemon_additional_db_stats",
                                  "Returns all additional DB Stats exported by OTRS")
        LOG.debug("Added additional db stats metric")
        metric.add_metric([], get_additional_db_stats())
        return metric

    def _metric_elastic_status_ok(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_elastic_status_ok",
                                   "Return 1 if elastic status is ok, 0 if not")
        LOG.debug("Added elastic status ok metric")
        metric.add_metric([], get_elastic_status())
        return metric

    def _metric_elastic_cluster_status(self) -> StateSetMetricFamily:
        metric = StateSetMetricFamily("otrs_elastic_cluster_status",
                                      "Return the cluster health with the traffic light schema used by ElasticSearch")
        LOG.debug("Added elastic cluster status metric")
        metric.add_metric([], get_elastic_overall_cluster_status())
        return metric

    def _metric_elastic_overall_node_status(self) -> StateSetMetricFamily:
        metric = StateSetMetricFamily("otrs_elastic_node_status",
                                      "Return the overall node health with the traffic light schema used by "
                                      "ElasticSearch")
        LOG.debug("Added elastic node status metric")
        metric.add_metric([], get_elastic_overall_nodes_status())
        return metric

    def _metric_elastic_all_nodes_status(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_elastic_failed_nodes",
                                  "Return the granular node health with the traffic light schema used by ElasticSearch")
        LOG.debug("Added elastic node status metric")
        metric.add_metric([], get_elastic_all_nodes_states())
        return metric

    def _metric_elastic_index_states(self) -> InfoMetricFamily:
        metric = InfoMetricFamily("otrs_elastic_index_states",
                                  "Returns available vs already indexed documents for each index")
        LOG.debug("Added elastic index status metric")
        metric.add_metric([], get_elastic_index_states())
        return metric

    def _metric_mail_queue_empty(self) -> GaugeMetricFamily:
        metric = GaugeMetricFamily("otrs_elastic_status_ok",
                                   "Return 1 if mail queue empty, 0 if not")
        LOG.debug("Added mail queue empty metric")
        metric.add_metric([], get_mail_queue_empty())
        return metric


def get_mail_queue_empty() -> int:
    """
    Determine if mail queue is empty, for mail issue diagnosis.

    :rtype: int
    :return: Empty (1), Non-Empty (0)
    """
    otrs_cli_out: str = subprocess.run(["/opt/otrs/bin/otrs.Console.pl", "Maint::Email::MailQueue", "--list"],
                                       stdout=subprocess.PIPE).stdout.decode("utf-8")
    if "Mail queue is empty." in otrs_cli_out:
        return 1
    else:
        return 0


def get_elastic_index_states() -> Dict[str:str]:
    """
    Parse the document indexing status for all OTRS Elastic Search Indices.

    :rtype: dict
    :return: Indices _avail and _indexed document count per index
    """
    # TODO str:int would make more sense here
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


def get_elastic_all_nodes_states() -> Dict[str:str]:
    """
    List all individual node states in the Elastic Search Traffic Light Pattern.

    :rtype: dict
    :return: Mapping from node name to state
    """
    otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
    nodes: List[str] = re.compile(r"^\s+\|\s+Node\s+\|\s+(\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
    states: List[str] = re.compile(r"^\s+\|\s+Status\s+\|\s+(\w+\W\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
    node_to_state = {n: s for n, s in zip(nodes, states)}
    return node_to_state


def get_elastic_overall_nodes_status() -> dict:
    """
    Determine a general status for the nodes from looking at all nodes individually. "On-line" is good / expected,
    everything else might lead to errors.

    :rtype: dict
    :return: Mapping from Red, Yellow, Green to "Truthyness"
    """
    # TODO figure out why this yields: otrs_elastic_node_status{otrs_elastic_node_status="Green"} 1.0
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


def get_elastic_overall_cluster_status() -> dict:
    """
    Retrieve the overall Elastic Cluster Status from OTRS.

    :rtype: dict
    :return: Mapping from Red, Yellow, Green to "Truthyness"
    """
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


def get_elastic_status() -> int:
    """
    Analogous to the get_db_status() method. Retrieve if connection to ElasticSearch Cluster is up or not.

    :rtype: int
    :return: Connection Status (True/False)
    """
    otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
    if "Connection successful." in otrs_cli_out:
        return 1
    else:
        return 0


def get_additional_db_stats() -> Dict[str:str]:
    """
    The OTRS DB Check retrieves additional stats (takes a long time) which we print here.

    :rtype: Dict[str:str]
    :return: Mapping of Checks (i.e. their names) to their results
    """
    otrs_cli_out: str = call_otrs_cli("Maint::Database::Check")
    matching: List[str] = re.compile(r"^(\w[\w\s]+)\: (.*)$", re.MULTILINE).findall(otrs_cli_out)
    return {match[0].lower().replace(" ", "_"):
            match[1].strip(" ").strip("(").strip(")").replace(" (", " - ")
            for match in matching}


def get_db_status() -> int:
    """
    Determine via OTRS CLI if the database is connected properly or not.

    :rtype: int
    :return: Connection okay or not okay
    """
    otrs_cli_out: str = call_otrs_cli("Maint::Database::Check")
    if "Connection successful." in otrs_cli_out:
        return 1
    else:
        return 0


def get_failing_crons() -> Dict[str:str]:
    """
    Get the names of all cron jobs that are marked with "Fail" and return those in a dictionary.

    :rtype: Dict[str:str]
    :return: Mapping of cron job names to the word "failed"
    """
    otrs_cli_out: str = call_otrs_cli("Maint::Daemon::Summary")
    matching: List[str] = re.compile(r"^\s*\|\s*([A-Za-z0-9]+)\s*\|\s*[\s\d:\-]*\|\s*Fail", re.MULTILINE).findall(
        otrs_cli_out)
    return {match: "failed" for match in matching}


def get_job_success_rate() -> float:
    """
    Return the OTRS Daemon Total Job Success Rate, i.e. count all occurrences of the word "Fail" in the output and
    divide by the total amount of jobs (which are determined by adding the jobs marked as "Success".

    :rtype: float
    :return: OTRS Daemon Job Success Rate
    """
    otrs_cli_out: str = call_otrs_cli("Maint::Daemon::Summary")
    daemon_success = otrs_cli_out.count("Success")
    daemon_total = daemon_success + otrs_cli_out.count("Fail")
    return daemon_success / daemon_total


def is_config_valid() -> int:
    """
    Parse the output of the Config Check and determine if said config is valid or not.

    :rtype: int
    :return: valid / not valid config
    """
    otrs_cli_out: str = call_otrs_cli("Admin::Config::ListInvalid")
    if "All settings are valid." in otrs_cli_out:
        return 1
    else:
        return 0


def get_mail_fetcher_errors() -> int:
    """
    Determine on some manually gathered strings issues within the Daemon logs regarding mail processing.

    :rtype: int
    :return: Occurrence of the mail errors in the current open file
    """
    error_count = 0
    for line in log:
        if any(s in line for s in ["Got no email",
                                   "S/MIME",
                                   "Could not re-process email",
                                   "PostMaster"]):
            error_count += 1
    return error_count
