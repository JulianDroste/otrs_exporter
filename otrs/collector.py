from typing import List

from prometheus_client import Gauge, Info, Enum
from pygtail import Pygtail
import subprocess
import re
import settings

log: Pygtail = Pygtail(settings.LOGWATCH)


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

    def _metric_mail_error_count(self) -> Gauge:
        metric = Gauge("otrs_mail_error", "Determine via OTRS logs whether there are issues with E-Mail")
        for line in log:
            if any(s in line for s in ["Got no email",
                                       "S/MIME",
                                       "Could not re-process email",
                                       "PostMaster"]):
                metric.inc()
        return metric

    def _metric_config_valid(self) -> Gauge:
        metric = Gauge("otrs_config_valid", "Is the OTRS Config valid or not?")
        otrs_cli_out: str = call_otrs_cli("Admin::Config::ListInvalid")
        if "All settings are valid." in otrs_cli_out:
            metric.set(1)
        else:
            metric.set(0)
        return metric

    def _metric_daemon_summary(self) -> Gauge:
        metric = Gauge("otrs_daemon_summary", "The OTRS Daemon Success Rate")
        otrs_cli_out: str = call_otrs_cli("Maint::Daemon::Summary")
        daemon_success = otrs_cli_out.count("Success")
        daemon_total = daemon_success + otrs_cli_out.count("Fail")
        metric.set(daemon_success / daemon_total)
        return metric

    def _metric_failing_crons(self) -> Info:
        metric = Info("otrs_daemon_cron_jobs", "This tells us about all failing OTRS Daemon Cron Jobs")
        otrs_cli_out: str = call_otrs_cli("Maint::Daemon::Summary")
        matching: List[str] = re.compile(r"^\s*\|\s*([A-Za-z0-9]+)\s*\|\s*[\s\d:\-]*\|\s*Fail", re.MULTILINE).findall(
            otrs_cli_out)
        metric.info({match: "failed" for match in matching})
        return metric

    def _metric_db_status_ok(self) -> Gauge:
        metric = Gauge("otrs_db_status_ok", "This tells us about all failing OTRS Daemon Cron Jobs")
        otrs_cli_out: str = call_otrs_cli("Maint::Database::Check")
        if "Connection successful." in otrs_cli_out:
            metric.set(1)
        else:
            metric.set(0)
        return metric

    def _metric_db_additional_stats(self) -> Info:
        metric = Info("otrs_daemon_additional_db_stats", "This tells us about all additional DB stats gathered by OTRS")
        otrs_cli_out: str = call_otrs_cli("Maint::Database::Check")
        matching: List[str] = re.compile(r"^(\w[\w\s]+)\: (.*)$", re.MULTILINE).findall(otrs_cli_out)
        metric.info({match[0].lower().replace(" ", "_"):
                         match[1].strip(" ").strip("(").strip(")").replace(" (", " - ")
                     for match in matching})
        return metric

    def _metric_elastic_status_ok(self) -> Gauge:
        metric = Gauge("otrs_elastic_status_ok", "This tells us about all failing OTRS Daemon Cron Jobs")
        otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
        if "Connection successful." in otrs_cli_out:
            metric.set(1)
        else:
            metric.set(0)
        return metric

    def _metric_elastic_cluster_status(self) -> Enum:
        metric = Enum("otrs_elastic_cluster_status",
                      "This tells us about all ElasticSearch Cluster Health",
                      states=["red", "yellow", "green"])
        otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
        matching: List[str] = re.compile(r"^\s+\|\s+Status\s+\|\s+(\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
        for match in matching:
            metric.state(match.lower())
        return metric

    def _metric_elastic_overall_node_status(self) -> Enum:
        metric = Enum("otrs_elastic_node_status",
                      "This tells us about the overall ElasticSearch Node Health",
                      states=["red", "yellow", "green"])
        otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
        states: List[str] = re.compile(r"^\s+\|\s+Status\s+\|\s+(\w+\W\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
        unique_status = set(states)
        if "On-line" not in unique_status:
            metric.state("red")
        # If one node goes bad, the overall node state should turn bad
        elif len(unique_status) > 1:
            metric.state("yellow")
        else:
            metric.state("green")
        return metric

    def _metric_elastic_all_nodes_status(self) -> Info:
        metric = Info("otrs_elastic_failed_nodes", "This tells us about all ElasticSearch nodes")
        otrs_cli_out: str = call_otrs_cli("Maint::DocumentSearch::Check")
        nodes: List[str] = re.compile(r"^\s+\|\s+Node\s+\|\s+(\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
        states: List[str] = re.compile(r"^\s+\|\s+Status\s+\|\s+(\w+\W\w+)\s+\|$", re.MULTILINE).findall(otrs_cli_out)
        node_to_state = {n: s for n, s in zip(nodes, states)}
        metric.info(node_to_state)
        return metric

    def _metric_elastic_index_states(self) -> Info:
        metric = Info("otrs_elastic_index_states", "This tells us about all ElasticSearch nodes")
        otrs_cli_out: str = subprocess.run(["/opt/otrs/bin/otrs.Console.pl",
                                            "Maint::DocumentSearch::IndexManagement", "--index-status", "all"],
                                           stdout=subprocess.PIPE).stdout.decode("utf-8")
        indices: List[str] = re.compile(r"^\s+\|\s+(\w+)\s+\|\s+(\d+)\s+\|\s+(\d+)\s+\|.*$", re.MULTILINE).findall(
            otrs_cli_out)
        indices_dict = {re.sub(r'(?<!^)(?=[A-Z])', '_', index[0]).lower(): {"indexed": index[2], "avail": index[1]} for
                        index in indices}
        metric.info(indices_dict)
        return metric

    def _metric_mail_queue_empty(self) -> Gauge:
        metric = Gauge("otrs_elastic_status_ok", "This tells us about all failing OTRS Daemon Cron Jobs")
        otrs_cli_out: str = subprocess.run(["/opt/otrs/bin/otrs.Console.pl", "Maint::Email::MailQueue", "--list"],
                                           stdout=subprocess.PIPE).stdout.decode("utf-8")
        if "Mail queue is empty." in otrs_cli_out:
            metric.set(1)
        else:
            metric.set(0)
        return metric
