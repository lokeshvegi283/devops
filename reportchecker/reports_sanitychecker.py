import os
import subprocess
import concurrent.futures
import argparse
import re
import sys
from datetime import datetime, timedelta
from collections import defaultdict

# ----------------- CONFIGURATION -----------------
# NSE Holidays for 2026 (YYYYMMDD)
NSE_HOLIDAYS_2026 = {
    "20260115", # Municipal Corporation Election
    "20260126", # Republic Day
    "20260303", # Holi
    "20260326", # Shri Ram Navami
    "20260331", # Shri Mahavir Jayanti
    "20260403", # Good Friday
    "20260414", # Dr. Baba Saheb Ambedkar Jayanti
    "20260501", # Maharashtra Day
    "20260528", # Bakri Id
    "20260626", # Muharram
    "20260914", # Ganesh Chaturthi
    "20261002", # Mahatma Gandhi Jayanti
    "20261020", # Dussehra
    "20261110", # Diwali-Balipratipada
    "20261124", # Prakash Gurpurb Sri Guru Nanak Dev
    "20261225"  # Christmas
}

# Hardcoded Weekends for 2026 (YYYYMMDD)
WEEKENDS_2026 = {
    # January
    "20260103", "20260110", "20260117", "20260124", "20260131", "20260104", "20260111", "20260118", "20260125",
    # February
    "20260207", "20260214", "20260221", "20260228", "20260201", "20260208", "20260215", "20260222",
    # March
    "20260307", "20260314", "20260321", "20260328", "20260301", "20260308", "20260315", "20260322", "20260329",
    # April
    "20260404", "20260411", "20260418", "20260425", "20260405", "20260412", "20260419", "20260426",
    # May
    "20260502", "20260509", "20260516", "20260523", "20260530", "20260503", "20260510", "20260517", "20260524", "20260531",
    # June
    "20260606", "20260613", "20260620", "20260627", "20260607", "20260614", "20260621", "20260628",
    # July
    "20260704", "20260711", "20260718", "20260725", "20260705", "20260712", "20260719", "20260726",
    # August
    "20260801", "20260808", "20260815", "20260822", "20260829", "20260802", "20260809", "20260816", "20260823", "20260830",
    # September
    "20260905", "20260912", "20260919", "20260926", "20260906", "20260913", "20260920", "20260927",
    # October
    "20261003", "20261010", "20261017", "20261024", "20261031", "20261004", "20261011", "20261018", "20261025",
    # November
    "20261107", "20261114", "20261121", "20261128", "20261101", "20261108", "20261115", "20261122", "20261129",
    # December
    "20261205", "20261212", "20261219", "20261226", "20261206", "20261213", "20261220", "20261227"
}

# Master list of all days to skip
SKIP_DATES_2026 = NSE_HOLIDAYS_2026.union(WEEKENDS_2026)

# ANSI Color Codes
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# ----------------- HELPER FUNCTIONS -----------------
def parse_inst_details(inst_str, token):
    """Parses Dump Instrument Details into standard Bhavcopy CSV key."""
    try:
        parts = inst_str.strip().split()
        if len(parts) >= 2:
            date_str = parts[0]
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            exp_dt = dt.strftime("%d-%b-%Y").upper() 
            
            rest = parts[1]
            if rest.endswith("CE") or rest.endswith("PE"):
                strike = float(rest[:-2])
                opt_typ = rest[-2:].upper()
            else:
                strike = float(rest)
                opt_typ = "XX" 
                
            symbol = "NIFTY"
            prefix_match = re.match(r"^([A-Za-z\&]+)\d", token)
            if prefix_match:
                symbol = prefix_match.group(1).upper()
            
            return f"{symbol}_{exp_dt}_{strike}_{opt_typ}"
    except Exception:
        pass
    return None

def safe_float_eq(v1, v2):
    try:
        return abs(float(v1) - float(v2)) < 0.001
    except ValueError:
        return v1 == v2

def get_remote_cmd_output(ip, cmd):
    """Executes SSH command with strict timeouts to prevent thread freezing."""
    disable_strict = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q -o ConnectTimeout=5 -o BatchMode=yes"
    full_cmd = f"ssh {disable_strict} infra@{ip} \"{cmd}\""
    try:
        return subprocess.check_output(full_cmd, shell=True, stderr=subprocess.STDOUT, timeout=15).decode().strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return ""

def format_column(matches, total, err_msg="", inverse_logic=False):
    """Formats output text and applies red highlighting securely without breaking column alignment."""
    width = 38
    if total == 0:
        text = f"Matched: 0/0 ({err_msg})"
        return f"{RED}{text.ljust(width)}{RESET}"
    
    if inverse_logic:
        # Col 1 logic: It is an anomaly if it completely matches (Copied)
        if matches == total:
            text = f"Matched: {matches}/{total} (COPIED)"
            return f"{RED}{text.ljust(width)}{RESET}"
        else:
            return f"Matched: {matches}/{total}".ljust(width)
    else:
        # Col 2 & 3 logic: It is an anomaly if there is a mismatch
        if matches == total:
            return f"Matched: {matches}/{total}".ljust(width)
        else:
            mismatch_cnt = total - matches
            text = f"Matched: {matches}/{total} (Mis: {mismatch_cnt})"
            return f"{RED}{text.ljust(width)}{RESET}"

# ----------------- CORE DATA EXTRACTION & COMPARISON -----------------
def check_trade_data(hostname, ip, date):
    srv_base = "/home/infra"
    tr_path = f"{srv_base}/archives/eod_files/trade_reports/eod_{date}_{ip}.stat"
    bhav_path = f"{srv_base}/archives/eod_files/trade_reports_bhavcopy/eod_{date}_{hostname}.stat"
    dump_path = f"{srv_base}/app_dumps/nse_order_monitor/dump/eod_dump_{date}.csv"
    
    # ---------------- DYNAMIC NAS PATH SEARCH ----------------
    nas_bases = ["/home/nas", "/home/nas2", "/home/nas3", "/home/nas4vol3"]
    nas_csv_path = None
    
    for base in nas_bases:
        potential_path = f"{base}/utils/processed/contract_files/nse_files_{date}/bhavcopy.csv"
        if os.path.exists(potential_path):
            nas_csv_path = potential_path
            break  # Stop checking once we find the file
    # ---------------------------------------------------------

    tr_cmd = f"if [ -f {tr_path} ]; then awk 'NF>0 && !/Account/ && \\$2!=\\\"\\\" {{print \\$2 \\\",\\\" \\$10}}' {tr_path}; fi"
    bhav_cmd = f"if [ -f {bhav_path} ]; then awk 'NF>0 && !/Account/ && \\$2!=\\\"\\\" {{print \\$2 \\\",\\\" \\$10}}' {bhav_path}; fi"
    
    # Dump pulls Col 21 (MTMPrice) AND Col 19 (LTP)
    dump_cmd = f"if [ -f {dump_path} ]; then awk -F, '!/Exchange/ && \\$3!=\\\"\\\" {{print \\$3 \\\",\\\" \\$21 \\\",\\\" \\$19}}' {dump_path}; fi"
    dump_map_cmd = f"if [ -f {dump_path} ]; then awk -F, '!/Exchange/ && \\$3!=\\\"\\\" {{print \\$3 \\\",\\\" \\$17}}' {dump_path}; fi"
    
    # 1. Fetch Remote Data
    tr_dict = {}
    for line in get_remote_cmd_output(ip, tr_cmd).split('\n'):
        parts = line.split(',')
        if len(parts) >= 2: tr_dict[parts[0].strip()] = parts[1].strip()

    bhav_stat_dict = {}
    for line in get_remote_cmd_output(ip, bhav_cmd).split('\n'):
        parts = line.split(',')
        if len(parts) >= 2: bhav_stat_dict[parts[0].strip()] = parts[1].strip()

    dump_mtm_dict = {}
    dump_ltp_dict = {}
    for line in get_remote_cmd_output(ip, dump_cmd).split('\n'):
        parts = line.split(',')
        if len(parts) >= 3:
            tok = parts[0].strip()
            dump_mtm_dict[tok] = parts[1].strip()
            dump_ltp_dict[tok] = parts[2].strip()

    dump_map = {}
    for line in get_remote_cmd_output(ip, dump_map_cmd).split('\n'):
        parts = line.split(',')
        if len(parts) >= 2:
            tok = parts[0].strip()
            mapped_key = parse_inst_details(parts[1].strip(), tok)
            if mapped_key: dump_map[tok] = mapped_key

    # 2. Fetch Local NAS Data (Multiply by 100 for paise formatting)
    csv_dict = {}
    if nas_csv_path:
        # Col 9 is CLOSE, Col 10 is SETTLE_PR
        cmd = f"awk -F, '!/INSTRUMENT/ && $2!=\"\" {{print $2 \",\" $3 \",\" $4 \",\" $5 \",\" $10 \",\" $9}}' {nas_csv_path}"
        try:
            out = subprocess.check_output(cmd, shell=True).decode().strip()
            for line in out.split('\n'):
                parts = line.split(',')
                if len(parts) >= 6:
                    sym, exp, strike, opt, settle, close_pr = [p.strip() for p in parts]
                    try:
                        key = f"{sym.upper()}_{exp.upper()}_{float(strike)}_{opt.upper()}"
                        settle_val = float(settle) * 100 if settle else 0.0
                        close_val = float(close_pr) * 100 if close_pr else 0.0
                        csv_dict[key] = (settle_val, close_val)
                    except ValueError:
                        pass
        except subprocess.CalledProcessError:
            pass

    # 3. Comparisons
    # C1: TR vs Bhavcopy
    c1_match, c1_tot, e1 = 0, 0, ""
    if tr_dict and bhav_stat_dict:
        shared = set(tr_dict.keys()) & set(bhav_stat_dict.keys())
        c1_tot = len(shared)
        if c1_tot == 0: e1 = "No Shared Sym"
        else: c1_match = sum(1 for k in shared if safe_float_eq(tr_dict[k], bhav_stat_dict[k]))
    else:
        m = []
        if not tr_dict: m.append("TR")
        if not bhav_stat_dict: m.append("Bhav")
        e1 = "Miss: " + "&".join(m)

    # C2: TR vs Dump (Check MTMPrice -> Fallback to LTP)
    c2_match, c2_tot, e2 = 0, 0, ""
    fallback_logs = []
    
    if tr_dict and dump_mtm_dict:
        shared = set(tr_dict.keys()) & set(dump_mtm_dict.keys())
        c2_tot = len(shared)
        if c2_tot == 0: e2 = "No Shared Sym"
        else:
            for k in shared:
                tr_val = tr_dict[k]
                mtm_val = dump_mtm_dict[k]
                ltp_val = dump_ltp_dict.get(k, "0")
                
                # Check MTMPrice first
                if safe_float_eq(tr_val, mtm_val):
                    c2_match += 1
                else:
                    # MTMPrice failed, check LTP fallback
                    if safe_float_eq(tr_val, ltp_val):
                        c2_match += 1
                        fallback_logs.append(f"{YELLOW}MTM Mismatch (Used LTP) - {k}: TR={tr_val} | Dump_MTM={mtm_val} | Dump_LTP={ltp_val}{RESET}")
                    else:
                        fallback_logs.append(f"{RED}Complete Mismatch - {k}: TR={tr_val} | Dump_MTM={mtm_val} | Dump_LTP={ltp_val}{RESET}")
    else:
        m = []
        if not tr_dict: m.append("TR")
        if not dump_mtm_dict: m.append("Dump")
        e2 = "Miss: " + "&".join(m)

    # C3: Bhavcopy vs CSV (Check SETTLE_PR -> Fallback to CLOSE)
    c3_match, c3_tot, e3 = 0, 0, ""
    c3_fallback_logs = []
    if bhav_stat_dict and dump_map and csv_dict:
        shared = set(bhav_stat_dict.keys()) & set(dump_map.keys())
        for k in shared:
            mapped_key = dump_map[k]
            if mapped_key in csv_dict:
                c3_tot += 1
                bhav_val = bhav_stat_dict[k]
                settle_val, close_val = csv_dict[mapped_key]
                
                # Check SETTLE_PR first
                if safe_float_eq(bhav_val, settle_val):
                    c3_match += 1
                else:
                    # SETTLE_PR failed, check CLOSE fallback
                    if safe_float_eq(bhav_val, close_val):
                        c3_match += 1
                        c3_fallback_logs.append(f"{YELLOW}CSV Mismatch (Used CLOSE) - {k}: Bhav={bhav_val} | CSV_SETTLE={settle_val} | CSV_CLOSE={close_val}{RESET}")
                    else:
                        c3_fallback_logs.append(f"{RED}CSV Complete Mismatch - {k}: Bhav={bhav_val} | CSV_SETTLE={settle_val} | CSV_CLOSE={close_val}{RESET}")
        
        if c3_tot == 0: e3 = "No Shared Sym"
    else:
        m = []
        if not bhav_stat_dict: m.append("Bhav")
        if not dump_map: m.append("Dump")
        if not csv_dict: m.append("CSV")
        e3 = "Miss: " + "&".join(m)

    return hostname, ip, date, c1_match, c1_tot, e1, c2_match, c2_tot, e2, c3_match, c3_tot, e3, fallback_logs, c3_fallback_logs

# ----------------- MAIN RUNNER -----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel Symbol-by-Symbol MTM verifier.")
    parser.add_argument("-H", "--hostname", required=True, help="Target Hostname (e.g. hft-nse-io-VL-1)")
    parser.add_argument("-I", "--ip", required=True, help="Target IP Address (e.g. 192.168.30.117)")
    parser.add_argument("-s", "--start", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("-e", "--end", default=None)
    args = parser.parse_args()

    if args.end is None: args.end = args.start
    
    target_host = args.hostname
    target_ip = args.ip

    try:
        start_dt = datetime.strptime(args.start, "%Y%m%d")
        end_dt = datetime.strptime(args.end, "%Y%m%d")
    except ValueError:
        print("Error: Dates must be in YYYYMMDD format.")
        exit(1)

    # Filter out non-trading days (Strictly checks hardcoded list)
    valid_dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y%m%d")
        
        # If the date is not in our master SKIP list, add it to execution queue
        if date_str not in SKIP_DATES_2026:
            valid_dates.append((current_dt, date_str))
            
        current_dt += timedelta(days=1)

    if not valid_dates:
        print("No valid trading dates found in the specified range.")
        exit(0)

    # Initialize data trackers
    anomaly_data = { 
        "TR vs Bhavcopy (Copied / 100% Match)": [], 
        "TR vs Dump (Mismatch with both MTM/LTP)": [], 
        "Bhav vs CSV (Settle Price Mismatch)": [] 
    }
    missing_data = { "TR vs Bhavcopy": [], "TR vs Dump": [], "Bhav vs CSV": [] }
    results_buffer = []

    print(f"\n{GREEN}[+] Booting Parallel Execution Engine for {len(valid_dates)} Trading Days...{RESET}\n")

    # Run all dates concurrently using ThreadPool (max 10 to protect SSH Daemon limits)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_date = {
            executor.submit(check_trade_data, target_host, target_ip, d_str): d_dt 
            for d_dt, d_str in valid_dates
        }
        
        for future in concurrent.futures.as_completed(future_to_date):
            d_dt = future_to_date[future]
            try:
                res = future.result()
                results_buffer.append((d_dt, res))
                
                # Live terminal feedback (Uses sys.stderr so it doesn't break > output redirects)
                date_completed = res[2]
                sys.stderr.write(f"\r  [✔] Processed Date: {date_completed} ({len(results_buffer)}/{len(valid_dates)})")
                sys.stderr.flush()
            except Exception as e:
                print(f"\n{RED}Error on date {d_dt.strftime('%Y%m%d')}: {e}{RESET}")

    sys.stderr.write("\n\n")

    # Sort results chronologically for clean output
    results_buffer.sort(key=lambda x: x[0])

    # Print sequential output
    for d_dt, res in results_buffer:
        hostname, ip, date_str, m1, t1, e1, m2, t2, e2, m3, t3, e3, fallback_logs, c3_fallback_logs = res
        
        print(f"==========================================================================================================================================")
        print(f"Symbol-by-Symbol MTM Check for DATE: {date_str}  [{d_dt.strftime('%A')}]")
        print(f"==========================================================================================================================================")
        print(f"{'Hostname':<17} | {'TR vs Bhavcopy (MTM)':<38} | {'TR vs Dump (MTMPrice/LTP)':<38} | {'Bhav vs bhavcopy.csv':<38}")
        print("-" * 140)
        
        str1 = format_column(m1, t1, e1, inverse_logic=True)
        str2 = format_column(m2, t2, e2, inverse_logic=False)
        str3 = format_column(m3, t3, e3, inverse_logic=False)
        
        print(f"{hostname:<17} | {str1} | {str2} | {str3}")
        
        # Combine C2 and C3 fallback logs for cleanly printing underneath the main line
        all_logs = fallback_logs + c3_fallback_logs
        
        if all_logs:
            for log in all_logs[:10]:
                print(f"                  ↳ {log}")
            if len(all_logs) > 10:
                print(f"                  ↳ {RED}... and {len(all_logs) - 10} more symbols.{RESET}")

        print("-" * 140 + "\n")
        
        # Populate Tracking Dictionaries
        if t1 == 0: missing_data["TR vs Bhavcopy"].append((d_dt, e1))
        elif m1 == t1: anomaly_data["TR vs Bhavcopy (Copied / 100% Match)"].append(d_dt)

        if t2 == 0: missing_data["TR vs Dump"].append((d_dt, e2))
        elif m2 != t2: anomaly_data["TR vs Dump (Mismatch with both MTM/LTP)"].append(d_dt)

        if t3 == 0: missing_data["Bhav vs CSV"].append((d_dt, e3))
        elif m3 != t3: anomaly_data["Bhav vs CSV (Settle Price Mismatch)"].append(d_dt)

    # ----------------- FINAL REPORT -----------------
    print("=" * 80)
    print(" 📊 FINAL ANOMALY & MISSING DATA REPORT (Grouped by Month) ")
    print("=" * 80)
    
    total_anomalies = sum(len(dates) for dates in anomaly_data.values())
    if total_anomalies > 0:
        print(f"\n{RED}[!] ANOMALIES DETECTED (Copied Files or Price Mismatches){RESET}")
        for category, dates in anomaly_data.items():
            if not dates: continue
            print(f"\n📌 {category}:")
            month_groups = defaultdict(list)
            for d in sorted(dates):
                month_groups[d.strftime("%B %Y")].append(d.strftime("%Y-%m-%d"))
            for month, day_list in month_groups.items():
                print(f"    - {month}: {RED}{', '.join(day_list)}{RESET}")
    else:
        print("\n[+] No anomalies found across available files.")

    total_missing = sum(len(items) for items in missing_data.values())
    if total_missing > 0:
        print(f"\n{RED}[!] MISSING FILES OR ZERO SHARED SYMBOLS{RESET}")
        for category, items in missing_data.items():
            if not items: continue
            print(f"\n📌 {category} (Matched: 0/0 State):")
            month_groups = defaultdict(list)
            for d, err in sorted(items, key=lambda x: x[0]):
                month_groups[d.strftime("%B %Y")].append(f"{d.strftime('%Y-%m-%d')} [{err}]")
            for month, day_list in month_groups.items():
                print(f"    - {month}: {RED}{', '.join(day_list)}{RESET}")
                
    print("\n" + "=" * 80)
