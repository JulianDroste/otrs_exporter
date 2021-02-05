import logging
import time
from otrs.collector import OtrsConnector
from prometheus_client import start_http_server, REGISTRY, Summary
import settings


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s]: %(message)s")


if __name__ == '__main__':
    if settings.DEBUG:
        logging.getLogger().setLevel(logging.DEBUG)

    start_http_server(settings.PORT, settings.IP)
    try:
        import systemd.daemon
        from systemd.daemon import notify, Notification
        systemd.daemon.notify(Notification.READY)
    except ImportError:
        logging.warning("Only partial systemd integration")
    logging.info(f"OTRS Prometheus Exporter started on {settings.IP}:{settings.PORT}")

    otrs_collector = OtrsConnector()

    REGISTRY.register(otrs_collector)

    while True:
        time.sleep(1)
