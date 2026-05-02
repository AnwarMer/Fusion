"""
Download missing echo DICOM frames for the multimodal ECG+Echo dataset.
Reads final_dataset.csv, checks all 3 echo frames (ED, Mid, ES) per row,
downloads only what's missing.
"""

import os, sys, time, threading, getpass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from tqdm import tqdm

# =============== CONFIGURATION — edit these ===============
PHYSIONET_USERNAME = "ibrahimhachemi"
PHYSIONET_PASSWORD = "jC&.L;]VpCrE:K6"   # or "" to be prompted

CSV_PATH  = r"C:\Users\anwme\Desktop\preprocessing\final_dataset.csv"
ECHO_DIR  = r"C:\Users\anwme\Desktop\Datasets\Physionet\Physionet-ECHO"

WORKERS   = 6
RETRIES   = 4
TIMEOUT   = 90
LOG_PATH  = r"C:\Users\anwme\Desktop\preprocessing\download_missing_log.txt"
# ==========================================================

BASE       = "https://physionet.org"
LOGIN_URL  = f"{BASE}/login/"
ECHO_BASE  = f"{BASE}/files/mimic-iv-echo/1.0/"
PROBE_URL  = f"{ECHO_BASE}LICENSE.txt"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0 Safari/537.36")


def login(username, password):
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    r = s.get(LOGIN_URL, timeout=TIMEOUT); r.raise_for_status()
    csrf = s.cookies.get("csrftoken")
    if not csrf: raise RuntimeError("No CSRF token from /login/")
    resp = s.post(LOGIN_URL,
        data={"csrfmiddlewaretoken": csrf, "username": username,
              "password": password, "next": "/"},
        headers={"Referer": LOGIN_URL, "Origin": BASE},
        timeout=TIMEOUT, allow_redirects=True)
    if "sessionid" not in s.cookies:
        raise RuntimeError("Login failed — no session cookie set.")
    if resp.url.rstrip("/").endswith("/login"):
        raise RuntimeError("Login failed — bounced back to /login/")
    probe = s.get(PROBE_URL, timeout=TIMEOUT)
    if probe.status_code != 200:
        raise RuntimeError(f"Login OK but PROBE failed ({probe.status_code}). "
                           "Did you sign the MIMIC-IV-Echo DUA?")
    print(f"[OK] Logged in, verified access to {PROBE_URL}")
    return s.cookies


_thread_local = threading.local()
def _get_session(cookies):
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        s.cookies.update(cookies)
        _thread_local.session = s
    return _thread_local.session


def download_file(url, dest, cookies):
    dest = Path(dest)
    if dest.exists() and dest.stat().st_size > 0:
        return "skip", 0

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    session = _get_session(cookies)
    last_msg = ""

    for attempt in range(1, RETRIES + 1):
        try:
            with session.get(url, timeout=TIMEOUT, stream=True) as resp:
                if resp.status_code == 200:
                    nbytes = 0
                    with open(tmp, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=1 << 16):
                            if chunk:
                                f.write(chunk); nbytes += len(chunk)
                    if nbytes == 0:
                        tmp.unlink(missing_ok=True)
                        last_msg = "Empty response"
                    else:
                        os.replace(tmp, dest)
                        return "ok", nbytes
                elif resp.status_code == 403:
                    return "forbidden", 0
                elif resp.status_code == 404:
                    return "not_found", 0
                else:
                    last_msg = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            last_msg = f"Network: {e}"
        tmp.unlink(missing_ok=True)
        if attempt < RETRIES:
            time.sleep(min(2 ** attempt, 30))
    return "failed", 0


def csv_path_to_local(csv_rel_path, base_dir):
    """Convert 'files/p19/p19971226/s.../XXX.dcm' to local disk path."""
    parts = csv_rel_path.replace('\\', '/').split('/')
    return Path(base_dir) / Path(*parts[2:])


def main():
    username = PHYSIONET_USERNAME
    password = PHYSIONET_PASSWORD or getpass.getpass(f"PhysioNet password for '{username}': ")

    if not Path(CSV_PATH).is_file():
        sys.exit(f"[FATAL] CSV not found: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df):,} rows from {CSV_PATH}")

    # Collect all (url, dest) pairs for the 3 echo frames per row
    all_tasks = []
    for _, row in df.iterrows():
        for col in ['echo_path_ED', 'echo_path_Mid', 'echo_path_ES']:
            rel = row[col].strip('/')
            url = f"{BASE}/{rel}" if rel.startswith("files/mimic") else f"{ECHO_BASE}{rel}"
            dest = csv_path_to_local(rel, ECHO_DIR)
            all_tasks.append((url, dest))

    # Filter to only missing (dedupe too, in case of duplicates)
    seen = set()
    missing = []
    already = 0
    for url, dest in all_tasks:
        key = str(dest)
        if key in seen: continue
        seen.add(key)
        if dest.exists() and dest.stat().st_size > 0:
            already += 1
        else:
            missing.append((url, dest))

    total = len(missing)
    print(f"Total echo files expected  : {len(seen):,}")
    print(f"Already downloaded         : {already:,}")
    print(f"Missing (to download)      : {total:,}")
    if total == 0:
        print("Nothing to do!"); return

    try:
        cookies = login(username, password)
    except Exception as e:
        sys.exit(f"[FATAL] {e}")

    counts = {"ok": 0, "skip": 0, "forbidden": 0, "not_found": 0, "failed": 0}
    total_bytes = 0
    failed_entries = []
    interrupted = False

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(download_file, url, dest, cookies): (url, dest)
                       for url, dest in missing}
            with tqdm(total=total, unit="file", ncols=100) as bar:
                for fut in as_completed(futures):
                    url, dest = futures[fut]
                    try:
                        status, nbytes = fut.result()
                    except Exception as e:
                        status, nbytes = "failed", 0
                        failed_entries.append(f"{url} -> Exception: {e}")
                    counts[status] = counts.get(status, 0) + 1
                    total_bytes += nbytes
                    if status not in ("ok", "skip"):
                        failed_entries.append(f"[{status}] {url}")
                    bar.set_postfix_str(f"{total_bytes/(1024**2):,.0f} MB | "
                                        f"ok={counts['ok']} fail={counts['failed']+counts['not_found']}",
                                        refresh=False)
                    bar.update(1)
    except KeyboardInterrupt:
        interrupted = True
        print("\n[!] Interrupted. Re-run to resume (already-downloaded files will be skipped).")

    with open(LOG_PATH, "w", encoding="utf-8") as f:
        f.write(f"Download log — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"CSV: {CSV_PATH}\nBreakdown: {counts}\n")
        f.write(f"Bytes: {total_bytes:,}  ({total_bytes/(1024**2):,.1f} MB)\n\n")
        if interrupted: f.write("INTERRUPTED\n\n")
        if failed_entries:
            f.write("=== FAILED ===\n" + "\n".join(failed_entries) + "\n")

    print("\n" + "="*60)
    print(f"Downloaded : {counts['ok']:,}")
    print(f"Skipped    : {counts['skip']:,}")
    print(f"Failed     : {counts['failed'] + counts['not_found'] + counts['forbidden']:,}")
    print(f"Data       : {total_bytes/(1024**2):,.1f} MB")
    print(f"Log        : {LOG_PATH}")


if __name__ == "__main__":
    main()