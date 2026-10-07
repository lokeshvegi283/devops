#!/usr/bin/python3
import os
import sys
import json
import urllib.request
import subprocess
import concurrent.futures

# ----------------- CONFIGURATION -----------------
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwFTob8btlRGFQFoPrjfY7dAVSySBry9jJXHzc9sTrQhy32XtYPcRVSwAlQoFgv6M1-0A/exec"
CHAT_WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQA_KlONyc/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=lQg2k2wE6Lj-4zBmVDDLtqk66xUMNPjr0b-9g_RwIX8"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-q", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes"]

REPORT_MAPPING = {
    "eod_reports": "EOD Report",
    "trade_reports": "Trade Report",
    "eod_reports_bhavcopy": "EOD Report Bhavcopy",
    "trade_reports_bhavcopy": "Trade Report Bhavcopy"
}

# ----------------- HELPER FUNCTIONS -----------------
def clean_ssh_output(raw_text):
    valid_lines = []
    for line in raw_text.split('\n'):
        line = line.strip()
        if not line: continue
        if any(b in line for b in ["Plutus Research", "*****", "authorized user", "being monitored", "Welcome to", "Disconnect immediately"]):
            continue
        valid_lines.append(line)
    return valid_lines[-1] if valid_lines else ""

def run_remote_cmd(ip, cmd, timeout=10):
    ssh_cmd = ["ssh"] + SSH_OPTS + [f"infra@{ip}", cmd]
    try:
        res = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=timeout)
        return res.stdout
    except Exception:
        return ""

def get_servers(date_str):
    dns_path = f'/home/nas/utils/etchosts/{date_str}_dns.txt'
    servers = []
    ignored = {'hft-nse-so-AH-9', 'hft-nse-cm-SB-1', 'hft-nse-io-NJ-1', 'hft-nse-io-NJ-2', 'hft-nse-io-NJ-3'}
    try:
        with open(dns_path, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2:
                    ip, host = parts[0], parts[1]
                    if any(x in host for x in ['hft-nse-so', 'hft-nse-io', 'hft-nse-cm', 'hft-nse-sq']):
                        if host not in ignored:
                            servers.append((ip, host))
        servers.extend([('192.168.170.18', 'hft-bse-sq-IN-1'), ('192.168.170.17', 'hft-bse-io-VL-2')])
    except Exception as e:
        print(f"DNS Read Error: {e}")
    return sorted(servers)

# ----------------- CORE LOGIC -----------------
def get_archive_trade_counts(ip, host, date_str):
    ors_pattern = 'bseorsfo' if 'bse' in host else 'nse_.._ors'
    
    remote_script = """
    ors_used=$(grep -v '\\[%s\\]' /home/infra/prod/lt/app_organize/applist 2>/dev/null | grep -a '%s' | head -n 1 | awk '{print $1}')
    if [ -z "$ors_used" ]; then
        echo "0,0"
        exit 0
    fi

    trade_archive_path="/home/infra/archives/trade_archive/%s/$ors_used/trades.gz"
    order_logger_path="/home/infra/archives/log_archive/%s/order_logger/log"
    
    trade_arc=0
    if [ -f "$trade_archive_path" ]; then
        trade_arc=$(zgrep -a 'respType=Trade' "$trade_archive_path" 2>/dev/null | wc -l)
    fi

    log_count=0
    for ol in "$order_logger_path"/O*; do
        if [ -f "$ol" ]; then
            spd=$(zgrep -a 'respType=Trade|symbol=SPD-' "$ol" 2>/dev/null | wc -l)
            logs=$(zgrep -a 'respType=Trade' "$ol" 2>/dev/null | wc -l)
            net=$((logs - spd))
            log_count=$((log_count + net))
        fi
    done

    echo "$trade_arc,$log_count"
    """ % (ors_pattern, ors_pattern, date_str, date_str)
    
    cmd = ["ssh"] + SSH_OPTS + [f"infra@{ip}", remote_script]
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=300)
        output_lines = [line for line in res.stdout.strip().splitlines() if line]
        output = output_lines[-1] if output_lines else "0,0"
        
        if "," in output:
            trade_arc_str, log_count_str = output.split(',')
            return int(trade_arc_str), int(log_count_str)
        return 0, 0
    except Exception:
        return 0, 0

def check_target_file_and_mtm(ip, host, date_str, raw_type):
    is_sq = 'sq' in host
    target_folder = f"{raw_type}_overnight" if is_sq else raw_type
    
    ip_file = f"/home/infra/archives/eod_files/{target_folder}/eod_{date_str}_{ip}.stat"
    host_file = f"/home/infra/archives/eod_files/{target_folder}/eod_{date_str}_{host}.stat"
    
    # Check IP file first, fallback to Host file
    cmd = """
    if [ -f "%s" ]; then 
        target="%s"
    elif [ -f "%s" ]; then
        target="%s"
    else 
        echo "missing"
        exit 0
    fi

    size=$(stat -c %%s "$target")
    if [ "$size" -gt 600 ]; then
        awk '/_Agg/ {print $9}' "$target" | tail -n 1
    else
        if /usr/sbin/lsof "$target" >/dev/null 2>&1 || lsof "$target" >/dev/null 2>&1 || fuser "$target" >/dev/null 2>&1; then
            echo "generating"
        else
            echo "failed"
        fi
    fi
    """ % (ip_file, ip_file, host_file, host_file)
    
    res = clean_ssh_output(run_remote_cmd(ip, cmd, timeout=10))
    
    status = "Missing"
    agg_trades = "N/A"
    
    if res == "generating":
        status = "Generating..."
    elif res == "failed":
        status = "Failed (<=600b)"
    elif res != "missing" and res:
        status = "Present"
        agg_trades = res if res.isdigit() else "0"

    mtm_anomalies = 0
    base_main = "eod_reports" if "eod" in raw_type else "trade_reports"
    base_bhav = "eod_reports_bhavcopy" if "eod" in raw_type else "trade_reports_bhavcopy"
    
    f_main_folder = f"/home/infra/archives/eod_files/{base_main}_overnight" if is_sq else f"/home/infra/archives/eod_files/{base_main}"
    f_bhav_folder = f"/home/infra/archives/eod_files/{base_bhav}_overnight" if is_sq else f"/home/infra/archives/eod_files/{base_bhav}"
    
    f_main_ip = f"{f_main_folder}/eod_{date_str}_{ip}.stat"
    f_main_host = f"{f_main_folder}/eod_{date_str}_{host}.stat"
    
    f_bhav_ip = f"{f_bhav_folder}/eod_{date_str}_{ip}.stat"
    f_bhav_host = f"{f_bhav_folder}/eod_{date_str}_{host}.stat"
    
    # Check IP file first, fallback to Host file for MTM extraction
    tr_cmd = "if [ -f " + f_main_ip + " ]; then awk 'NF>0 && !/Account/ {print $2 \",\" $10}' " + f_main_ip + "; elif [ -f " + f_main_host + " ]; then awk 'NF>0 && !/Account/ {print $2 \",\" $10}' " + f_main_host + "; fi"
    bhav_cmd = "if [ -f " + f_bhav_ip + " ]; then awk 'NF>0 && !/Account/ {print $2 \",\" $10}' " + f_bhav_ip + "; elif [ -f " + f_bhav_host + " ]; then awk 'NF>0 && !/Account/ {print $2 \",\" $10}' " + f_bhav_host + "; fi"
    
    tr_out = run_remote_cmd(ip, tr_cmd)
    bhav_out = run_remote_cmd(ip, bhav_cmd)
    
    tr_lines = [line.strip() for line in tr_out.split('\n') if ',' in line]
    bhav_lines = [line.strip() for line in bhav_out.split('\n') if ',' in line]
    
    tr_dict = {line.split(',')[0]: line.split(',')[1] for line in tr_lines}
    for line in bhav_lines:
        sym, mtm = line.split(',')
        if sym in tr_dict and tr_dict[sym] != mtm:
            mtm_anomalies += 1

    return status, agg_trades, mtm_anomalies

def process_server(ip, host, date_str, raw_type):
    arc_trades, log_trades = get_archive_trade_counts(ip, host, date_str)
    status, agg_trades, mtm_anomalies = check_target_file_and_mtm(ip, host, date_str, raw_type)
    
    return {
        "ip": ip,
        "host": host,
        "log_trades": log_trades,
        "arc_trades": arc_trades,
        "status": status,
        "agg_trades": agg_trades,
        "mtm_mismatches": mtm_anomalies
    }

# ----------------- APPS SCRIPT GENERATOR -----------------
def generate_and_send_chat_alert(results, date_str, raw_type, mail_send):
    report_title = REPORT_MAPPING.get(raw_type, raw_type)
    
    if mail_send != 'Y':
        print("\n[+] Script finished. (mail_send != 'Y', alert not sent)")
        return

    payload = {
        "webhook_url": CHAT_WEBHOOK_URL,
        "report_type": report_title,
        "date": date_str,
        "results": results
    }
    
    try:
        req = urllib.request.Request(
            APPS_SCRIPT_URL,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                print(f"\nData sent to Apps Script successfully. Chat alert triggered.")
            else:
                print(f"\nFailed to send to Apps Script. Status Code: {response.status}")
    except Exception as e:
        print(f"\nFailed to send alert: {e}")

# ----------------- MAIN RUNNER -----------------
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 report_check_new.py <Y/N> <YYYYMMDD> <report_type>")
        print("Example: python3 report_check_new.py Y 20260827 eod_reports")
        sys.exit(1)
        
    mail_send = sys.argv[1]
    target_date = sys.argv[2]
    raw_type = sys.argv[3]

    if raw_type not in REPORT_MAPPING:
        print(f"Error: Invalid report type '{raw_type}'. Must be one of: {list(REPORT_MAPPING.keys())}")
        sys.exit(1)

    servers = get_servers(target_date)
    print(f"Starting specific trade checks ({raw_type}) for {len(servers)} servers...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_server, ip, host, target_date, raw_type): host for ip, host in servers}
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

    sorted_results = sorted(results, key=lambda x: x['host'])
    generate_and_send_chat_alert(sorted_results, target_date, raw_type, mail_send)
