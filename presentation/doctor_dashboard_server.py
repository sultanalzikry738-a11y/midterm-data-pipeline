#!/usr/bin/env python3
from __future__ import annotations
import json, os, re, shutil, subprocess, sys, tempfile, threading, time, uuid, webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

HOST='127.0.0.1'
PORT=int(os.getenv('DOCTOR_DASHBOARD_PORT','8765'))
SAFE_DB=os.getenv('DOCTOR_TEST_DB','midterm_doctor_test')
HERE=Path(__file__).resolve().parent
PROJECT_ROOT=HERE.parent
HTML_FILE=HERE/'hybrid_big_data_dashboard_pro.html'
MAIN_PY=PROJECT_ROOT/'src'/'main.py'
REPORTS_DIR=PROJECT_ROOT/'reports'
CANONICAL_RESULTS=REPORTS_DIR/'results.json'
RUN_LOCK=threading.Lock()

def send_json(h,payload,status=200):
    raw=json.dumps(payload,ensure_ascii=False).encode('utf-8')
    h.send_response(status); h.send_header('Content-Type','application/json; charset=utf-8'); h.send_header('Content-Length',str(len(raw))); h.send_header('Cache-Control','no-store'); h.end_headers(); h.wfile.write(raw)

def first(d,*keys,default=None):
    for k in keys:
        if d.get(k) is not None: return d[k]
    return default

def parse_router(stdout):
    def grab(p):
        m=re.search(p,stdout,re.MULTILINE); return m.group(1).strip() if m else None
    return {
        'file_name':grab(r'^Input file\s*:\s*(.+)$'),
        'file_size_mb':grab(r'^File size\s*:\s*([0-9.]+)\s*MB'),
        'threshold_mb':grab(r'^Threshold\s*:\s*([0-9.]+)\s*MB'),
        'engine':grab(r'^Engine\s*:\s*(\S+)'),
        'router_reason':grab(r'^Reason\s*:\s*(.+)$'),
        'run_id':grab(r'^Run ID\s*:\s*([0-9a-fA-F-]{20,})'),
    }

def locate_results(stdout,started_at):
    matches=re.findall(r'(?:Results|Metrics saved to)\s*:\s*(.+?\.json)\s*$',stdout,re.MULTILINE)
    for raw in reversed(matches):
        p=Path(raw.strip().strip('"'))
        if p.exists(): return p
    candidates=sorted(REPORTS_DIR.glob('results_*.json'),key=lambda p:p.stat().st_mtime,reverse=True)
    for p in candidates:
        if p.stat().st_mtime>=started_at-2: return p
    if CANONICAL_RESULTS.exists() and CANONICAL_RESULTS.stat().st_mtime>=started_at-2: return CANONICAL_RESULTS
    return None

def normalize_results(data,router,result_file):
    wm=data.get('write_metrics') if isinstance(data.get('write_metrics'),dict) else {}
    cc=data.get('consistency_check') if isinstance(data.get('consistency_check'),dict) else {}
    valid=first(data,'count_valid','valid_count')
    corrected=first(data,'count_corrected','corrected_count')
    quarantine=first(data,'count_quarantine','quarantine_count')
    raw=first(data,'loaded_raw','raw_loaded','raw_count')
    rows=first(data,'rows_read','read_rows',default=raw)
    inserted=first(data,'inserted','inserted_count',default=first(wm,'inserted'))
    updated=first(data,'updated','updated_count',default=first(wm,'updated'))
    unchanged=first(data,'unchanged','unchanged_count',default=first(wm,'unchanged'))
    status=first(cc,'status',default=first(data,'consistency_status','quality_status'))
    return {
        'run_id':first(data,'run_id',default=router.get('run_id')),
        'file_name':first(data,'source_file','filename','file_name',default=router.get('file_name')),
        'file_size_mb':first(data,'file_size_mb',default=router.get('file_size_mb')),
        'threshold_mb':router.get('threshold_mb'),
        'engine':first(data,'engine_used','engine',default=router.get('engine')),
        'router_reason':router.get('router_reason'),
        'rows_read':rows,'raw_loaded':raw,'valid':valid,'corrected':corrected,'quarantine':quarantine,
        'inserted':inserted,'updated':updated,'unchanged':unchanged,'consistency':status,
        'results_file':str(result_file) if result_file else None,
    }

def choose_csv_file():
    if os.name=='nt':
        ps_script=(
            "$ErrorActionPreference='Stop'; Add-Type -AssemblyName System.Windows.Forms; "
            "$d=New-Object System.Windows.Forms.OpenFileDialog; "
            "$d.Title='Select CSV file for Doctor Test'; "
            "$d.Filter='CSV files (*.csv)|*.csv|All files (*.*)|*.*'; "
            "$d.Multiselect=$false; "
            "$d.InitialDirectory=[Environment]::GetFolderPath('Desktop'); "
            "if($d.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK){ "
            "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; Write-Output $d.FileName }"
        )
        try:
            p=subprocess.run(['powershell.exe','-NoProfile','-STA','-Command',ps_script],capture_output=True,text=True,encoding='utf-8',errors='replace')
            raw=(p.stdout or '').strip()
            if p.returncode==0 and raw: return Path(raw)
        except Exception:
            pass
    try:
        import tkinter as tk
        from tkinter import filedialog
        root=tk.Tk(); root.withdraw(); root.attributes('-topmost',True)
        chosen=filedialog.askopenfilename(title='Select CSV file',filetypes=[('CSV files','*.csv'),('All files','*.*')])
        root.destroy(); return Path(chosen) if chosen else None
    except Exception as exc:
        raise RuntimeError(f'Could not open file picker: {exc}') from exc

class Handler(SimpleHTTPRequestHandler):
    server_version='DoctorDashboard/1.1'
    def log_message(self,fmt,*args): sys.stdout.write('[dashboard] '+(fmt%args)+'\n')
    def do_GET(self):
        path=urlparse(self.path).path
        if path=='/api/health':
            problems=[]
            if not MAIN_PY.exists(): problems.append(f'Missing: {MAIN_PY}')
            if not HTML_FILE.exists(): problems.append(f'Missing: {HTML_FILE}')
            send_json(self,{'ok':not problems,'error':'; '.join(problems) if problems else None,'project_root':str(PROJECT_ROOT),'main_py':str(MAIN_PY),'mongo_db':SAFE_DB,'python':sys.executable,'python_version':sys.version.split()[0]},200 if not problems else 500); return
        if path=='/api/select-file':
            try:
                selected=choose_csv_file()
                if not selected: send_json(self,{'ok':False,'cancelled':True}); return
                selected=selected.resolve()
                if not selected.exists() or not selected.is_file(): send_json(self,{'ok':False,'error':'Selected path is not a file.'},400); return
                if selected.suffix.lower()!='.csv': send_json(self,{'ok':False,'error':'Only .csv files are accepted.'},400); return
                send_json(self,{'ok':True,'path':str(selected),'name':selected.name,'size_bytes':selected.stat().st_size}); return
            except Exception as exc:
                send_json(self,{'ok':False,'error':str(exc)},500); return
        if path in ('/','/index.html','/hybrid_big_data_dashboard_pro.html'):
            if not HTML_FILE.exists(): self.send_error(404,'Dashboard HTML not found'); return
            raw=HTML_FILE.read_bytes(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.send_header('Content-Length',str(len(raw))); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(raw); return
        self.send_error(404,'Not Found')
    def do_POST(self):
        if urlparse(self.path).path!='/api/run': send_json(self,{'ok':False,'error':'Unknown API endpoint'},404); return
        if not MAIN_PY.exists(): send_json(self,{'ok':False,'error':f'main.py not found: {MAIN_PY}'},500); return
        if not RUN_LOCK.acquire(blocking=False): send_json(self,{'ok':False,'error':'A pipeline run is already in progress.'},409); return
        backup=None
        try:
            length=int(self.headers.get('Content-Length','0')); raw=self.rfile.read(length); payload=json.loads(raw.decode('utf-8')) if raw else {}
            selected=Path(str(payload.get('path',''))).expanduser().resolve()
            if not selected.exists() or not selected.is_file(): send_json(self,{'ok':False,'error':'Selected CSV file no longer exists.'},400); return
            if selected.suffix.lower()!='.csv': send_json(self,{'ok':False,'error':'Only .csv files are accepted.'},400); return
            if CANONICAL_RESULTS.exists():
                backup=Path(tempfile.gettempdir())/f'midterm_results_backup_{uuid.uuid4().hex}.json'; shutil.copy2(CANONICAL_RESULTS,backup)
            env=os.environ.copy(); env['MONGO_DB_NAME']=SAFE_DB; env['PYTHONIOENCODING']='utf-8'
            cmd=[sys.executable,str(MAIN_PY),'--input',str(selected),'--execute']; started=time.time()
            proc=subprocess.run(cmd,cwd=str(PROJECT_ROOT),env=env,capture_output=True,text=True,encoding='utf-8',errors='replace')
            stdout=(proc.stdout or '')+(('\n'+proc.stderr) if proc.stderr else '')
            result_file=locate_results(stdout,started); data={}
            if result_file and result_file.exists():
                try: data=json.loads(result_file.read_text(encoding='utf-8-sig'))
                except Exception as exc: stdout+=f'\n[DASHBOARD] Could not parse results JSON: {exc}'
            router=parse_router(stdout); normalized=normalize_results(data,router,result_file)
            ok=proc.returncode==0 and 'HYBRID BIG DATA ELT PIPELINE: PASS' in stdout
            if not ok: send_json(self,{'ok':False,'error':f'Pipeline failed with exit code {proc.returncode}','stdout':stdout,'result':normalized},500); return
            send_json(self,{'ok':True,'stdout':stdout,'result':normalized})
        except Exception as exc:
            send_json(self,{'ok':False,'error':f'Dashboard backend error: {exc}'},500)
        finally:
            try:
                if backup and backup.exists(): shutil.copy2(backup,CANONICAL_RESULTS); backup.unlink(missing_ok=True)
            except Exception as exc: print(f'[dashboard] WARNING: could not restore results.json: {exc}')
            RUN_LOCK.release()

def main():
    if not HTML_FILE.exists(): raise SystemExit(f'Dashboard HTML missing: {HTML_FILE}')
    if not MAIN_PY.exists(): raise SystemExit(f'Project entrypoint missing: {MAIN_PY}')
    server=ThreadingHTTPServer((HOST,PORT),Handler); url=f'http://{HOST}:{PORT}/'
    print('='*68); print('HYBRID BIG DATA - DOCTOR TEST DASHBOARD'); print('='*68); print(f'Project : {PROJECT_ROOT}'); print(f'Python  : {sys.executable}'); print(f'Safe DB : {SAFE_DB}'); print(f'URL     : {url}'); print('Press Ctrl+C to stop the dashboard server.'); print('='*68)
    threading.Timer(0.8,lambda:webbrowser.open(url)).start()
    try: server.serve_forever()
    except KeyboardInterrupt: print('\nDashboard stopped.')
    finally: server.server_close()
if __name__=='__main__': main()
