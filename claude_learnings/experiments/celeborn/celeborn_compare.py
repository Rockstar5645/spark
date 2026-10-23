#!/usr/bin/env python3
"""Run one Spark script twice against the local standalone cluster, first with the built-in
sort shuffle and then with Celeborn, and compare where the shuffle bytes landed.

    experiments/celeborn/celeborn_compare.py experiments/celeborn/celeborn_groupby.scala

Source ../env.sh and run ../report/render_conf.sh cluster first. The Celeborn run uses
$SPARK_CONF_DIR/celeborn.conf (rendered from celeborn/conf-templates). See README.md here.

What to look for:
  * AKHIL_RESULT lines: groups/rows/hash must be IDENTICAL across runs (correctness).
  * peak bytes: with the built-in shuffle, the bytes go to spark_tmp/ (the executor's local disk).
    With Celeborn, spark_tmp/ should stay near zero and celeborn_data/ should grow instead.
"""
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(os.environ.get("EXTERN_ROOT") or Path(__file__).resolve().parents[4])
SPARK = ROOT / "spark"
SCRIPT = str(Path(sys.argv[1]).resolve())  # spark-shell runs from spark/, so make it absolute
LOGS = ROOT / "celeborn_logs"  # outside the repo
WATCH = {"spark_tmp": ROOT / "spark_tmp", "spark_work": ROOT / "spark_work",
         "celeborn_data": ROOT / "celeborn_data"}
CONF = Path(os.environ.get("SPARK_CONF_DIR") or ROOT / "spark_conf_cluster")
RUNS = {"builtin": [], "celeborn": ["--properties-file", str(CONF / "celeborn.conf")]}


def du(p):
    total = 0
    for f in p.rglob("*"):
        try:
            if f.is_file():
                total += f.stat().st_size
        except FileNotFoundError:
            pass  # shuffle files appear and disappear while we walk
    return total


def watch(peaks, stop):
    while not stop.is_set():
        for k, p in WATCH.items():
            peaks[k] = max(peaks[k], du(p))
        time.sleep(1)


LOGS.mkdir(parents=True, exist_ok=True)
for name, extra in RUNS.items():
    log = LOGS / f"{Path(SCRIPT).stem}-{name}.log"
    base = {k: du(p) for k, p in WATCH.items()}
    peaks = dict(base)
    stop = threading.Event()
    th = threading.Thread(target=watch, args=(peaks, stop), daemon=True)
    th.start()
    t = time.time()
    with open(log, "w") as f:
        p = subprocess.run(["./bin/spark-shell", *extra, "-i", SCRIPT], cwd=SPARK,
                           input=":quit\n", text=True, stdout=f, stderr=subprocess.STDOUT,
                           timeout=3600)
    stop.set()
    th.join()
    text = log.read_text(errors="replace")
    res = [l.strip() for l in text.splitlines() if "AKHIL_RESULT" in l]
    app = [l.split("app ID ")[-1].strip() for l in text.splitlines() if "app ID" in l]
    print(f"== {name}: rc={p.returncode} wall={time.time() - t:.0f}s app={app[:1]}")
    print(f"   log: {log}")
    print("  ", res[0] if res else "NO RESULT")
    print("   peak growth:", {k: f"{(peaks[k] - base[k]) / 2**30:.2f} GiB" for k in WATCH})
    errs = sorted({l.strip()[:180] for l in text.splitlines() if " ERROR " in l and "AKHIL" not in l})
    for e in errs[:5]:
        print("   !", e)
