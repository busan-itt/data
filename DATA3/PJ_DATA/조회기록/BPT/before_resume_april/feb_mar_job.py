"""2월~4월 BPT 조회. 2026-10-15 08:30 이전 완료 시에만 컴퓨터 종료."""
from pathlib import Path
import json, os, sqlite3, subprocess, sys
from datetime import datetime, timezone, timedelta

BASE=Path(__file__).resolve().parent
STATE=BASE/'crawl_state'
STATE.mkdir(exist_ok=True)
STATUS=STATE/'feb_mar_job_status.json'

def now(): return datetime.now(timezone(timedelta(hours=9))).isoformat(timespec='seconds')

def persist(data):
    data['updated_at']=now()
    STATUS.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')

def shutdown_delay(finished):
    cutoff=datetime(2026,10,15,8,30,tzinfo=timezone(timedelta(hours=9)))
    seconds=(cutoff-finished).total_seconds()
    return None if seconds<=0 else max(0,min(60,int(seconds)-1))

def wait_existing(pid):
    import ctypes
    from ctypes import wintypes
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
    kernel.OpenProcess.restype=wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes=[wintypes.HANDLE,wintypes.DWORD]
    kernel.CloseHandle.argtypes=[wintypes.HANDLE]
    handle=kernel.OpenProcess(0x100000,False,pid)
    if not handle: raise RuntimeError('기존 월 감독 프로세스를 확인할 수 없습니다.')
    try:
        while kernel.WaitForSingleObject(handle,15000)==258: pass
    finally: kernel.CloseHandle(handle)

def run(adopt_supervisor=None):
    data={'status':'running','pid':os.getpid(),'months':['2025-02','2025-03','2025-04'],
          'started_at':now(),'shutdown_after_success':True,'shutdown_cutoff':'2026-10-15T08:30:00+09:00'}
    persist(data)
    for month in data['months']:
        data['active_month']=month;persist(data)
        if month=='2025-02' and adopt_supervisor:
            data['supervisor_pid']=adopt_supervisor;persist(data)
            wait_existing(adopt_supervisor)
            code=0
        else:
            child=subprocess.Popen([sys.executable,'-u',str(BASE/'full_query_job.py'),
                                    '--service','BPT','--month',month],cwd=BASE)
            data['supervisor_pid']=child.pid;persist(data)
            code=child.wait()
        monthly=json.loads((STATE/('bpt_'+month.replace('-','_')+'_job_status.json')).read_text(encoding='utf-8'))
        if code!=0 or monthly.get('status')!='completed':
            data.update(status='stopped_with_issues',month_status=monthly.get('status'),exit_code=code)
            persist(data);return
        # 정상 상태뿐 아니라 대상별 저장 성공까지 재확인한다.
        import csv
        with (STATE/('targets_'+month.replace('-','_')+'.csv')).open(encoding='utf-8-sig',newline='') as f:
            reader=csv.reader(f);next(reader);targets={r[0] for r in reader}
        db=sqlite3.connect(f'file:{(STATE/"progress.sqlite3").as_posix()}?mode=ro',uri=True)
        ok={r[0] for r in db.execute("SELECT container_no FROM results WHERE service='BPT' AND status IN ('FOUND','FOUND_UNCLASSIFIED','NO_RESULT_RETURNED')")}
        db.close()
        if not targets or not targets<=ok:
            data['status']='verification_failed';persist(data);return
    finished=now();delay=shutdown_delay(datetime.fromisoformat(finished))
    if delay is None:
        data.update(status='completed_kept_on',finished_at=finished,shutdown_reason='08:30 cutoff reached')
        persist(data);return
    data.update(status='completed_shutdown_scheduled',finished_at=finished,shutdown_delay_seconds=delay);persist(data)
    subprocess.run(['shutdown.exe','/s','/t',str(delay),'/c','BPT 2025년 2월~4월 조회와 CSV 저장이 완료되어 컴퓨터를 종료합니다.'],check=True)

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--adopt-supervisor',type=int)
    args=parser.parse_args()
    try: run(args.adopt_supervisor)
    except BaseException as exc:
        persist({'status':'failed','error':f'{type(exc).__name__}: {exc}'})
        raise
