import logging
import os


ETL_ABS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

LOG_DIR = os.path.join(ETL_ABS_PATH, "logs")
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

OUTPUT_DIR = os.path.join(ETL_ABS_PATH, "output")
if not os.path.exists(OUTPUT_DIR):
    os.mkdir(OUTPUT_DIR)

LOGFILE = os.path.join(LOG_DIR, "tsg_etl.log")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()
fh = logging.FileHandler(LOGFILE)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s: %(message)s"))
logger.addHandler(fh)
