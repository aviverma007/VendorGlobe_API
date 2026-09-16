"""
Runs ONE fetch-and-store cycle, then exits.
Designed to be triggered repeatedly by Windows Task Scheduler
(instead of, or in addition to, app.py's continuous background writer).

Usage (called by Task Scheduler):
    py push_once.py

Logs to push_once.log in the same folder.
"""

import sys
import os
import traceback
from datetime import datetime

# Ensure this script's own folder is importable (needed for embeddable/
# isolated Python distributions, which don't add the script dir automatically).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db_writer

LOG_FILE = "push_once.log"


def log(message):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def main():
    try:
        db_writer.init()
        ins, upd, same = db_writer.fetch_and_store()
        log(f"OK - +{ins} new, ~{upd} updated, {same} unchanged")
    except Exception:
        log("ERROR:\n" + traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
