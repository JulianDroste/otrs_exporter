import os

PORT = int(os.getenv("OTRS_EXP_PORT", 9875))
IP = os.getenv("OTRS_EXP_IP", "0.0.0.0")
DEBUG = os.getenv("OTRS_EXP_DEBUG", False)
DB_USER = os.getenv("OTRS_EXP_DB_USER", "otrs")
DB_PW = os.getenv("OTRS_EXP_DB_PW")
DB_NAME = os.getenv("OTRS_EXP_DB_NAME", "otrs")
DB_HOST = os.getenv("OTRS_EXP_DB_HOST", "127.0.0.1")
LOGWATCH = os.getenv("OTRS_EXP_LOG_PATH", "/opt/otrs/var/log/Daemon/*")
