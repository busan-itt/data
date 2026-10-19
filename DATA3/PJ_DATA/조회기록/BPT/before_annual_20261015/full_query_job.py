"""DATA 프로젝트 전체 조회 감독 프로세스. UI 없이 로그와 진행 상태를 저장."""
from pathlib import Path
import argparse, csv, json, os, sqlite3, subprocess, sys, time
from datetime import datetime, timezone, timedelta

BASE=Path(__file__).resolve().parent
OUT=BASE/'crawl_state'
RESULTS=BASE/'crawl_results'
OUT.mkdir(parents=True,exist_ok=True)
STATE=OUT/'full_job_status.json'
LOG=OUT/'execution_full.log'
KST=timezone(timedelta(hours=9))
TARGET_IDS=None
SELECTED_SERVICES=None

def now():return datetime.now(KST).isoformat(timespec='seconds')

def counts():
    result=[]
    try:
        db=sqlite3.connect(f'file:{(OUT/"progress.sqlite3").as_posix()}?mode=ro',uri=True,timeout=5)
        grouped={}
        for number,service,status in db.execute('SELECT container_no,service,status FROM results'):
            if TARGET_IDS is not None and number not in TARGET_IDS: continue
            if SELECTED_SERVICES is not None and service not in SELECTED_SERVICES: continue
            key=(service,status);grouped[key]=grouped.get(key,0)+1
        for (service,status),n in sorted(grouped.items()):
            result.append({'service':service,'status':status,'count':n})
        db.close()
    except sqlite3.Error:pass
    return result

def write_state(state):
    state['updated_at']=now();state['counts']=counts()
    network_file=OUT/'network_status.json'
    if network_file.exists():
        try: state['network']=json.loads(network_file.read_text(encoding='utf-8'))
        except (ValueError,OSError): pass
    tmp=STATE.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')
    try:
        tmp.replace(STATE)
    except PermissionError:
        # Windows의 교체 권한 문제로 진행 감시가 종료되지 않도록 원래 파일에 저장.
        STATE.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')

def run(service="all",month=None):
    global STATE,LOG,TARGET_IDS,SELECTED_SERVICES
    services=["BCT","BPT"] if service=="all" else [service]
    if service=="BPT":
        STATE=OUT/"bpt_job_status.json"
        LOG=OUT/"execution_bpt.log"
    from crawl_bct_bpt import load_ids,select_month_ids
    targets=load_ids(BASE/'DGT_remaining_130522.csv')
    if month:
        targets=select_month_ids(targets,Path('C:/work/PJ4/DATA2/DGT_2025_anmual.csv'),month)
        STATE=OUT/('bpt_'+month.replace('-','_')+'_job_status.json')
        LOG=OUT/('execution_bpt_'+month.replace('-','_')+'.log')
    TARGET_IDS=set(targets);SELECTED_SERVICES=set(services)
    total=len(targets)
    if not total:raise ValueError('조회 대상이 없습니다.')
    state={'status':'starting','supervisor_pid':os.getpid(),'started_at':now(),
           'month':month,'input_count':total,'expected_service_results':total*len(services),
           'services':services,'delay_seconds':[0.5,1.2],
           'log_file':str(LOG),'output_dir':str(RESULTS)}
    env=os.environ.copy();env['PYTHONIOENCODING']='utf-8'
    write_state(state)
    try:
        with LOG.open('a',encoding='utf-8',buffering=1) as log:
            for attempt in range(1,4):
                log.write(f'\n=== FULL RUN {now()} attempt {attempt} ===\n');log.flush()
                command=[sys.executable,'-u',str(BASE/'crawl_bct_bpt.py'),
                    '--input',str(BASE/'DGT_remaining_130522.csv'),
                    '--output',str(RESULTS),'--state-dir',str(OUT),'--limit','0','--service',service,
                    '--delay-min','0.5','--delay-max','1.2','--export-every','1000','--retry-errors']
                if month:command.extend(['--month',month])
                child=subprocess.Popen(command,cwd=str(BASE),stdout=log,stderr=subprocess.STDOUT,env=env)
                state.update(status='running',attempt=attempt,worker_pid=child.pid)
                write_state(state)
                while child.poll() is None:
                    time.sleep(15)
                    write_state(state)
                code=child.returncode
                state['exit_code']=code
                if (OUT/'stop_requested.json').exists():
                    state.update(status='stopped_by_user',finished_at=now());write_state(state);return
                if code==0:
                    rows=[r for r in counts() if r['service'] in services];queried=sum(r['count'] for r in rows)
                    errors=sum(r['count'] for r in rows if r['status'] in ['LOOKUP_ERROR','RESPONSE_UNPARSED'])
                    state['status']=('completed' if queried==total*len(services) and errors==0
                                     else 'completed_with_issues' if queried==total*len(services) else 'incomplete')
                    state['finished_at']=now();write_state(state)
                    return
                # 접속 장애로 종료되면 저장된 체크포인트에서 최대 두 번 재개.
                if attempt<3:
                    state.update(status='retry_wait',retry_delay_seconds=60*attempt)
                    write_state(state)
                    time.sleep(60*attempt)
            state.update(status='failed',finished_at=now())
            write_state(state)
    except Exception as exc:
        state.update(status='failed',error=f'{type(exc).__name__}: {exc}',finished_at=now())
        write_state(state)
        raise

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--service',choices=['all','BCT','BPT'],default='all')
    parser.add_argument('--month')
    args=parser.parse_args()
    run(args.service,args.month)
