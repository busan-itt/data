"""DGT 명세 신규 수집. --prepare: 표본·예상·카카오, --run: 순차 수집·검증·종료."""
import argparse,calendar,csv,ctypes,gzip,hashlib,json,os,random,re,socket,sqlite3,subprocess,sys,threading,time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timedelta,timezone
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from pathlib import Path
from urllib.request import Request,build_opener,HTTPCookieProcessor
from urllib.error import HTTPError,URLError

KST=timezone(timedelta(hours=9));BASE=Path('C:/work/DATA')
STATE=BASE/'PJ_DATA/조회기록/DGT_명세_20261018'
OUT=BASE/'PJ_DATA/조회결과/DGT_명세조회'
PAGE='https://www.dgtbusan.com/DGT/esvc/cntr/cntrInfo'
HOST='https://www.dgtbusan.com'
LO=datetime(2024,11,1);HI=datetime(2026,3,1)
STOP=threading.Event();REPORT_END=threading.Event();STATUS_LOCK=threading.RLock()
STATUS={};CACHE={};CACHE_LOCK=threading.RLock();ALL_FIELDS=set();VESSEL_NAMES={};EXPORT_WORKER=False;EXPORT_PROCESS=None
csv.field_size_limit(2**30)
def now():return datetime.now(KST)
def stamp():return now().isoformat(timespec='seconds')
def atomic(path,data):
 path.parent.mkdir(parents=True,exist_ok=True);t=path.with_name(path.name+'.tmp')
 t.write_bytes(data)
 for _ in range(30):
  try:os.replace(t,path);return
  except PermissionError:time.sleep(.2)
 raise RuntimeError('파일 교체 잠금 지속: '+str(path))
def jwrite(p,v):atomic(p,json.dumps(v,ensure_ascii=False,indent=2).encode('utf-8'))
def norm(v):return re.sub('[^A-Z0-9]','',str(v).upper())
def dt(v):
 try:
  t=datetime.fromisoformat(str(v).strip().replace('/','-'))
  return t.astimezone(KST).replace(tzinfo=None) if t.tzinfo else t
 except (ValueError,TypeError):return None
def addmonths(t,n=3):
 m=t.month+n-1;y=t.year+m//12;m=m%12+1
 return t.replace(year=y,month=m,day=min(t.day,calendar.monthrange(y,m)[1]))
def within(a,b):return max(a,b)<=addmonths(min(a,b))
def valid(t):return t is not None and LO<=t<HI
def readcsv(p):
 with p.open(encoding='utf-8-sig',newline='') as f:yield from csv.DictReader(f)
def stopped():
 if STOP.is_set() or (STATE/'stop_requested.json').exists():raise InterruptedError('사용자 중단 요청')
def pause(seconds):
 until=time.monotonic()+seconds
 while time.monotonic()<until:
  stopped();time.sleep(min(1,max(0,until-time.monotonic())))
def persist(**kw):
 with STATUS_LOCK:
  STATUS.update(kw);STATUS['updated_at']=stamp();jwrite(STATE/'status.json',STATUS)
def internet():
 for host in ['www.microsoft.com','www.google.com']:
  try:
   with socket.create_connection((host,443),timeout=4):return True
  except OSError:pass
 return False
def wait_network():
 if internet():return False
 begin=stamp();persist(status='network_wait',network_wait_started_at=begin)
 while not internet():pause(random.uniform(30,60))
 persist(status='running',network_restored_at=stamp());return True
class Meta(HTMLParser):
 def __init__(self):super().__init__();self.meta={}
 def handle_starttag(self,tag,attrs):
  a=dict(attrs)
  if tag=='meta' and 'name' in a:self.meta[a['name']]=a.get('content','')
class Client:
 def __init__(self):self.opener=build_opener(HTTPCookieProcessor(CookieJar()));self.headers={};self.pool=ThreadPoolExecutor(max_workers=4)
 def refresh(self):
  with self.opener.open(Request(PAGE,headers={'User-Agent':'Mozilla/5.0'}),timeout=30) as f:s=f.read().decode('utf-8')
  parser=Meta();parser.feed(s);header=parser.meta.get('_csrf_header');token=parser.meta.get('_csrf')
  if not header or not token:raise ValueError('DGT 세션 생성 실패')
  self.headers={'User-Agent':'Mozilla/5.0','Referer':PAGE,'Content-Type':'application/json','Accept':'application/json',header:token}
 def post(self,path,body,cache=False,optional=False):
  key=hashlib.sha256((path+json.dumps(body,sort_keys=True)).encode()).hexdigest()
  if cache:
   with CACHE_LOCK:
    if key in CACHE:return CACHE[key]
  try:
   req=Request(HOST+path,data=json.dumps(body).encode() if body is not None else b'',headers=self.headers,method='POST')
   with self.opener.open(req,timeout=25) as f:value=json.load(f)
   answer={'availability':'응답 수신','response':value}
  except HTTPError as e:
   if optional and e.code in (401,403):answer={'availability':'공개 접근 제한','http_status':e.code,'response':None}
   else:raise RuntimeError(f'HTTP {e.code}: {path}') from None
  if cache:
   with CACHE_LOCK:CACHE[key]=answer
  return answer

def dbconnect():
 db=sqlite3.connect(STATE/'progress.sqlite3',timeout=30)
 db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA synchronous=FULL')
 db.execute('CREATE TABLE IF NOT EXISTS specimens (number TEXT PRIMARY KEY,status TEXT,attempts INTEGER,queried_at TEXT,raw_file TEXT,error TEXT,seconds REAL)')
 db.commit();return db
def dbr():return sqlite3.connect('file:'+(STATE/'progress.sqlite3').as_posix()+'?mode=ro',uri=True,timeout=10)
def loadraw(n):
 p=STATE/'raw'/n[:2]/(n+'.json.gz')
 if p.exists():
  with gzip.open(p,'rt',encoding='utf-8') as f:return json.load(f)
 return {'container_no':n,'queried_at':stamp(),'main':None,'steps':{},'step_errors':{},'attempts':0}
def saveraw(doc):
 n=doc['container_no'];p=STATE/'raw'/n[:2]/(n+'.json.gz')
 atomic(p,gzip.compress(json.dumps(doc,ensure_ascii=False).encode(),compresslevel=4));return p
def main_records(doc):
 if not doc.get('main'):return []
 return doc['main'].get('containers') or []
def requests_for(n,record):
 p=str(record.get('point',''));i=record.get('in') or {};o=record.get('out') or {};calls=[]
 def add(label,path,body,cache=False):calls.append((p+':'+label,path,body,cache))
 add('파손','/DGT/container/damage',{'filter':{'cntrNos':[n],'point':p}})
 add('통관','/DGT/emgt/rest/eService/container/customs',{'contNo':n,'point':p})
 add('홀드','/DGT/container/holdContainer',{'filter':{'cntrNos':[n]}},True)
 add('냉동','/DGT/container/reeferContainer',{'filter':{'cntrNo':n}},True)
 add('재조작','/DGT/document/rehandles',{'cntrNos':[n]},True)
 for side,detail in [('반입',i),('반출',o)]:
  v=detail.get('vvd') or ''
  if len(v)>=5:add(side+'선박일정','/DGT/berth/vesselSchedule',{'vessel':v[:4],'voyage':v[5:17]},True)
 v=i.get('vvd') or ''
 if len(v)>=5:add('본선작업예정','/DGT/document/vesselContainer',{'vessel':v[:4],'voyage':v[5:17],'cntrNos':[n]})
 billing_date=re.sub(r'[-:\s]','',str(o.get('outDate') or now().strftime('%Y-%m-%d %H:%M')))
 add('비용','/DGT/emgt/rest/eService/billing/cntrCashInfo',{'cntrNos':[{'cntrNo':n,'cntrPoint':p,'outDate':billing_date}]})
 if record.get('stamp')=='Gate Out' and record.get('fullEmpty')=='M':
  add('공컨테이너반환비용','/DGT/emgt/rest/eService/billing/cntrEmptyReturnInfo',{'cntrNo':n,'cntrPoint':int(p)+1})
 return calls
def collect(client,n,db):
 doc=loadraw(n);beg=time.monotonic()
 while doc['attempts']<3:
  stopped();doc['attempts']+=1;doc['queried_at']=stamp();issue=None
  try:
   if not client.headers:client.refresh()
   if doc['main'] is None:
    response=client.post('/DGT/container/containers',{'filter':{'cntrNos':[n],'stamp':['B','Y','O']}})['response']
    rr=response.get('containers') or []
    if int(response.get('searchedCount',-1))!=len(rr) or any(norm(r.get('cntrNo'))!=n for r in rr):raise ValueError('기본 명세 응답 불완전')
    doc['main']=response
   futures={}
   for record in main_records(doc):
    for key,path,body,cache in requests_for(n,record):
     if key in doc['steps']:continue
     futures[client.pool.submit(client.post,path,body,cache,True)]=(key,path,body)
   for future in as_completed(futures):
    key,path,body=futures[future]
    try:
     doc['steps'][key]={'path':path,'request':body,**future.result()};doc['step_errors'].pop(key,None)
    except Exception as e:doc['step_errors'][key]=str(e);issue=str(e)
  except Exception as e:issue=str(e)
  # Disconnected time never consumes a lookup attempt.
  if issue and not internet():
   doc['attempts']-=1;saveraw(doc);wait_network();client.headers={};continue
  if issue:client.headers={}
  raw=saveraw(doc)
  result='DONE' if not issue and not doc['step_errors'] else ('ERROR' if doc['attempts']>=3 else 'PARTIAL')
  db.execute('INSERT OR REPLACE INTO specimens VALUES (?,?,?,?,?,?,?)',(n,result,doc['attempts'],doc['queried_at'],str(raw),issue or '',time.monotonic()-beg));db.commit()
  pause(random.uniform(.7,1.5))
  if result in ('DONE','ERROR'):return doc
 # A previous interruption may have committed the final allowed attempt.
 raw=saveraw(doc);db.execute('INSERT OR REPLACE INTO specimens VALUES (?,?,?,?,?,?,?)',(n,'ERROR',doc['attempts'],doc['queried_at'],str(raw),'3회 시도 후 미해결',time.monotonic()-beg));db.commit();return doc

def build_targets():
 hp=BASE/'PJ_DATA/조회기록/HJNC_DGT/targets.json'
 hj=json.loads(hp.read_text(encoding='utf-8-sig'));hset=set(hj)
 href={n:[] for n in hj};bref={};term={}
 for r in readcsv(BASE/'PJ_DATA/HJNC_2025_annual.csv'):
  n=norm(r['컨테이너번호'])
  if n in hset and r.get('T/S 구분') in {'T','P','Z','S'}:
   href[n].extend(t.isoformat(' ') for t in (dt(r.get('반입시간')),dt(r.get('반출시간'))) if valid(t))
 for r in readcsv(BASE/'PJ_DATA/bpt_lookup.csv'):
  n=norm(r['컨테이너번호']);term[n]=r.get('이용부두','');bref.setdefault(n,[])
  for k in ['양하일시','반입일시','반출일시','적하일시','접안일시','출항일시','양하작업종료일시','본선작업종료일시']:
   t=dt(r.get(k));
   if valid(t):bref[n].append(t.isoformat(' '))
 plan={'created_at':stamp(),'HJNC':sorted(href),'BPT':sorted(bref),'references':{'HJNC':{n:sorted(set(v)) for n,v in href.items()},'BPT':{n:sorted(set(v)) for n,v in bref.items()}},'bpt_terminal':term,'new_unique_total':len(set(href)|set(bref)),'shared_ids':len(set(href)&set(bref)),'period_rule':'양쪽 작업시각 2024-11-01 이상 2026-03-01 미만, 가장 가까운 시각 차이 달력 3개월 이내(경계 포함). 운송 회차·직접 환적 확정 아님.'}
 jwrite(STATE/'targets.json',plan);return plan
def period(record,reference):
 ts=[]
 for group,key in [('in','inDate'),('in','stackDate'),('out','truckInDate'),('out','onChassisDate'),('out','outDate')]:
  t=dt((record.get(group) or {}).get(key))
  if valid(t):ts.append(t)
 rr=[dt(x) for x in reference];rr=[x for x in rr if valid(x)]
 if not ts:return False,None,'분석기간 내 DGT 작업시각 없음'
 if not rr:return False,None,'기준자료의 기간 내 작업시각 없음'
 pairs=[(abs((a-b).total_seconds()),a,b) for a in ts for b in rr]
 gap,a,b=min(pairs)
 return within(a,b),round(gap/86400,6),'3개월 이내' if within(a,b) else '작업시각 차이 3개월 초과'
def flatten(obj,prefix='',out=None):
 out={} if out is None else out
 if isinstance(obj,dict):
  for k,v in obj.items():flatten(v,(prefix+'.' if prefix else '')+str(k),out)
 elif isinstance(obj,list):out[prefix]=json.dumps(obj,ensure_ascii=False,separators=(',',':'))
 else:out[prefix]=obj
 return out
VHEAD=['컨테이너번호','대상그룹','BPT_이용부두','DGT_방문회차','조회상태','시도횟수','기간판정','최소시각차이_일','오류구분','오류내용','조회시각','원문파일','항목설명','판정설명']
def uncached_exported_rows(plan):
 db=dbr();entries={r[0]:r for r in db.execute('SELECT number,status,attempts,queried_at,raw_file,error,seconds FROM specimens')};db.close()
 for group in ['HJNC','BPT']:
  for n in plan[group]:
   if n not in entries:continue
   _,status,attempts,q,path,err,_=entries[n];doc=loadraw(n);rr=main_records(doc)
   if not rr:rr=[None]
   for r in rr:
    ok,gap,reason=period(r,plan['references'][group][n]) if r else (False,None,'명세 결과 없음')
    point=str(r.get('point','')) if r else ''
    failures={k:v for k,v in doc['step_errors'].items() if k.startswith(point+':')} if r else doc['step_errors']
    error_type='조회오류' if status=='ERROR' and (failures or not r) else ('' if ok else ('결과없음' if not r else '기간불일치'))
    # Global errors with no recorded detail key still belong in ERROR.
    if status=='ERROR' and not doc.get('step_errors'):error_type='조회오류'
    row={'컨테이너번호':n,'대상그룹':group,'BPT_이용부두':plan['bpt_terminal'].get(n,'') if group=='BPT' else '', 'DGT_방문회차':point,'조회상태':'일부 항목 조회 오류' if error_type=='조회오류' else '명세 조회 완료','시도횟수':attempts,'기간판정':reason,'최소시각차이_일':gap,'오류구분':error_type,'오류내용':json.dumps(failures,ensure_ascii=False) if failures else err if error_type=='조회오류' else '', '조회시각':q,'원문파일':path,'항목설명':'필드설명.csv 및 설명.txt 참조','판정설명':'기존 자료를 결과 행에 합치지 않음. 기간 이내는 직접 환적·같은 운송 확정이 아님.'}
    if r:
     flatten(r,'명세',row)
     for key,value in doc['steps'].items():
      if key.startswith(point+':'):flatten(value,'상세.'+key.split(':',1)[1],row)
     for side in ['in','out']:
      code=((r.get(side) or {}).get('vvd') or '')[:4]
      if code in VESSEL_NAMES:flatten(VESSEL_NAMES[code],'선박명.'+side,row)
     row['화면_파손표시']='비로그인 화면은 권한 제한(-). 반환 원문과 구분.'
    yield ('ERROR' if error_type else group),row
def exported_rows(plan):
 # Cache the fully preserved flattened rows. Existing gzip files need not be
 # reopened on every 1,000-number CSV snapshot.
 db=dbr();versions={n:json.dumps([s,a,q]) for n,s,a,q in db.execute('SELECT number,status,attempts,queried_at FROM specimens')};db.close()
 cache=sqlite3.connect(STATE/'export_cache.sqlite3',timeout=30)
 cache.execute('CREATE TABLE IF NOT EXISTS rows_cache (source_group TEXT,number TEXT,version TEXT,data BLOB,PRIMARY KEY(source_group,number))')
 stored={(g,n):(v,blob) for g,n,v,blob in cache.execute('SELECT source_group,number,version,data FROM rows_cache')}
 missing={g:[n for n in plan[g] if n in versions and ((g,n) not in stored or stored[g,n][0]!=versions[n])] for g in ['HJNC','BPT']}
 total=sum(map(len,missing.values()));processed=0;key=None;bucket=[]
 def flush(k,items):
  nonlocal processed
  if k is None:return
  g,n=k;blob=gzip.compress(json.dumps(items,ensure_ascii=False).encode(),compresslevel=3)
  cache.execute('INSERT OR REPLACE INTO rows_cache VALUES (?,?,?,?)',(g,n,versions[n],blob));stored[k]=(versions[n],blob);processed+=1
  if processed%250==0:
   cache.commit();jwrite(STATE/'export_progress.json',{'phase':'원문을 CSV용으로 준비','prepared':processed,'new_total':total,'updated_at':stamp()})
 try:
  subset={**plan,**missing}
  for dest,row in uncached_exported_rows(subset):
   current=(row['대상그룹'],row['컨테이너번호'])
   if current!=key:flush(key,bucket);key=current;bucket=[]
   bucket.append([dest,row])
  flush(key,bucket);cache.commit()
  for group in ['HJNC','BPT']:
   for n in plan[group]:
    if n not in versions:continue
    for dest,row in json.loads(gzip.decompress(stored[group,n][1])):yield dest,row
 finally:cache.close()
def export_inner(plan):
 fields=set();counts=Counter();unique={g:set() for g in ['HJNC','BPT','ERROR']}
 for group,row in exported_rows(plan):fields.update(row);counts[group]+=1;unique[group].add(row['컨테이너번호'])
 headers=VHEAD+sorted(fields-set(VHEAD));streams={};writers={}
 names={'HJNC':'DGT_HJNC.csv','BPT':'DGT_BPT.csv','ERROR':'ERROR.csv'}
 try:
  for g,name in names.items():
   streams[g]=(OUT/(name+'.tmp')).open('w',encoding='utf-8-sig',newline='');writers[g]=csv.DictWriter(streams[g],fieldnames=headers);writers[g].writeheader()
  for group,row in exported_rows(plan):writers[group].writerow(row)
 finally:
  for f in streams.values():f.close()
 for g,name in names.items():
  src=OUT/(name+'.tmp')
  for attempt in range(30):
   try:src.replace(OUT/name);break
   except PermissionError:time.sleep(.2)
  else:raise RuntimeError('CSV 교체 실패: '+name)
 with (OUT/'필드설명.csv').open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.writer(f);w.writerow(['열이름','설명','주의사항'])
  known={'cntrNo':'컨테이너번호','point':'DGT 방문 회차','isoCode':'ISO 크기·형식 코드','weight':'원문 중량(kg)','cntrClass':'DGT 화물 Class 원문','fullEmpty':'적재/공 상태 원문','operator':'선사 코드','in.inDate':'DGT 반입시각','in.stackDate':'장치시각','out.truckInDate':'차량 진입시각','out.onChassisDate':'상차 관련 시각: 운송방식과 함께 해석','out.outDate':'DGT 반출시각','in.inRego':'반입 차량번호','out.outRego':'반출 차량번호','in.vvd':'반입 선박 항차','out.vvd':'반출 선박 항차'}
  for key in headers:
   desc=known.get(key.removeprefix('명세.'),'DGT 응답 원문 경로. 목록은 JSON 문자열로 보존.' if key.startswith(('명세.','상세.','선박명.')) else key)
   w.writerow([key,desc,'빈값·미제공·공개 접근 제한은 없음/0으로 해석하지 말 것. 비용·홀드·냉동·재조작은 조회시점 응답으로 과거 회차와 다를 수 있음.'])
 result={'saved_at':stamp(),'rows':dict(counts),'unique_ids':{k:len(v) for k,v in unique.items()},'all_exported_unique':len(set().union(*unique.values()))}
 jwrite(OUT/'저장검증.json',result)
 jwrite(STATE/'export_status.json',{'status':'completed','result':result,'updated_at':stamp()})
 if not EXPORT_WORKER:persist(last_export=result)
 return result
def export(plan):
 import msvcrt
 lock=(STATE/'export.lock').open('a+b');lock.seek(0);lock.write(b'0');lock.flush();lock.seek(0)
 try:
  while True:
   try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1);break
   except OSError:stopped();time.sleep(1)
  jwrite(STATE/'export_progress.json',{'phase':'CSV 저장 시작','updated_at':stamp()})
  result=export_inner(plan)
  jwrite(STATE/'export_progress.json',{'phase':'CSV 저장 완료','updated_at':stamp()})
  return result
 finally:lock.close()
def schedule_export():
 global EXPORT_PROCESS
 if EXPORT_PROCESS is not None and EXPORT_PROCESS.poll() is None:return
 with (STATE/'export_stdout.log').open('a',encoding='utf-8') as stdout,(STATE/'export_stderr.log').open('a',encoding='utf-8') as stderr:
  EXPORT_PROCESS=subprocess.Popen([sys.executable,'-u',str(Path(__file__).resolve()),'--export-only'],stdout=stdout,stderr=stderr,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)|getattr(subprocess,'BELOW_NORMAL_PRIORITY_CLASS',0))
def sync_export_status():
 p=STATE/'export_status.json'
 if not p.exists():return
 try:
  result=json.loads(p.read_text(encoding='utf-8'))['result']
  if result['saved_at']>STATUS.get('last_export',{}).get('saved_at',''):STATUS['last_export']=result
 except (OSError,ValueError,KeyError):pass

def notify(text):
 sys.path.insert(0,str(BASE/'크롤링 코드/BPT'))
 import kakao_report as k
 # Reuse the already authorized app's registered link domain; only the message is DGT-specific.
 for attempt in range(3):
  try:
   k.send(text)
   jwrite(STATE/'kakao_report_status.json',{'status':'sent','last_sent_at':stamp(),'text':text[:200]});return True
  except Exception as e:
   jwrite(STATE/'kakao_report_status.json',{'status':'send_error','updated_at':stamp(),'error':str(e)})
   if attempt<2:time.sleep(2)
 return False
def progress_text(plan):
 sync_export_status()
 db=dbr();rows=list(db.execute("SELECT number,status,queried_at,seconds FROM specimens WHERE status IN ('DONE','ERROR')"));db.close()
 ids={r[0] for r in rows};h=len(ids&set(plan['HJNC']));b=len(ids&set(plan['BPT']));err=sum(r[1]=='ERROR' for r in rows)
 recent=[r[3] for r in rows if r[3] is not None and (now()-datetime.fromisoformat(r[2])).total_seconds()<1800]
 speed=sum(recent)/len(recent)+1.1 if recent else STATUS.get('seconds_per_number',5)
 remaining=(plan['new_unique_total']-len(ids))*speed;eta=now()+timedelta(seconds=remaining)
 persist(completed_unique=len(ids),hjnc_done=h,bpt_done=b,error_ids=err,estimated_finished_at=eta.isoformat(timespec='minutes'))
 return f'DGT 명세 {now():%m/%d %H:%M}\nHJNC {h:,}/{len(plan["HJNC"]):,}\nBPT {b:,}/{len(plan["BPT"]):,}\n조회오류 {err}개 / 상태 {STATUS.get("phase","")}\n잔여 약 {remaining/3600:.1f}시간\n예상 완료 {eta:%m/%d %H:%M}\n저장 검증·최종 보고 후 전원 종료'
def reporter(plan):
 last=now().strftime('%Y%m%d%H')+str(now().minute//30)
 while not REPORT_END.wait(10):
  slot=now().strftime('%Y%m%d%H')+str(now().minute//30)
  if slot!=last:
   try:notify(progress_text(plan))
   except Exception as e:persist(report_error=str(e))
   last=slot
def prepare(client,db):
 global VESSEL_NAMES
 plan=json.loads((STATE/'targets.json').read_text(encoding='utf-8')) if (STATE/'targets.json').exists() else build_targets()
 persist(status='preparing',phase='pilot',hjnc_total=len(plan['HJNC']),bpt_total=len(plan['BPT']),total_unique=plan['new_unique_total'],pid=os.getpid())
 try:
  client.refresh();names=client.post('/DGT/emgt/rest/eService/container/vesselName',None,True,True)
  jwrite(STATE/'vessel_names.json',names)
  VESSEL_NAMES={str(r.get('vesselCode',''))[:4]:r for r in (names.get('response') or {}).get('data',[]) if isinstance(r,dict)}
 except Exception as e:persist(vessel_name_note=str(e))
 pilot=[plan['HJNC'][int((len(plan['HJNC'])-1)*fraction)] for fraction in [0,.25,.5,.75,1]]
 times=[]
 for n in dict.fromkeys(pilot):
  r=db.execute('SELECT status,seconds FROM specimens WHERE number=?',(n,)).fetchone()
  if not r or r[0] not in ('DONE','ERROR'):
   collect(client,n,db);r=db.execute('SELECT status,seconds FROM specimens WHERE number=?',(n,)).fetchone()
  times.append(r[1])
 # Include normal random delay and allow a range for differing visit counts and endpoints.
 speed=max(1.5,sum(times)/len(times)+1.1)
 hsec=len(plan['HJNC'])*speed;bnew=len(set(plan['BPT'])-set(plan['HJNC']));bsec=bnew*speed
 estimates={'created_at':stamp(),'pilot_ids':pilot,'pilot_seconds':times,'seconds_per_number':speed,'HJNC_hours':hsec/3600,'BPT_additional_hours':bsec/3600,'total_hours':(hsec+bsec)/3600,'HJNC_expected':(now()+timedelta(seconds=hsec)).isoformat(timespec='minutes'),'all_expected':(now()+timedelta(seconds=hsec+bsec)).isoformat(timespec='minutes'),'note':'표본 5개 기반 초기 추정. 빈 이력·추가 회차·인터넷 대기·오류에 따라 변동. 30분 보고에서 갱신.'}
 jwrite(STATE/'estimates.json',estimates);persist(seconds_per_number=speed,estimates=estimates)
 msg=f'DGT 명세조회 시작 예정\nHJNC {len(plan["HJNC"]):,}개 → BPT {len(plan["BPT"]):,}개\n총 예상 {estimates["total_hours"]:.1f}시간(초기 표본 추정)\nHJNC 약 {hsec/3600:.1f}시간, 이후 BPT 약 {bsec/3600:.1f}시간\n정각·30분 보고, 저장 검증 후 노트북 종료'
 sent=notify(msg);persist(initial_kakao_sent=sent)
 export(plan)
 return plan
def final_verify(plan,saved):
 db=dbr();done={r[0] for r in db.execute("SELECT number FROM specimens WHERE status IN ('DONE','ERROR')")};db.close()
 assert done==set(plan['HJNC'])|set(plan['BPT']),'미처리 번호 존재'
 actual={};represented={'HJNC':set(),'BPT':set()}
 for name in ['DGT_HJNC.csv','DGT_BPT.csv','ERROR.csv']:
  count=0
  for r in readcsv(OUT/name):represented[r['대상그룹']].add(r['컨테이너번호']);count+=1
  actual[name]=count
 for group in represented:assert represented[group]==set(plan[group]),group+' 결과 번호 누락'
 jwrite(OUT/'최종저장검증.json',{'verified_at':stamp(),'verified':True,'rows':actual,'target_unique':len(done),'rule':plan['period_rule']})
 return actual
def run(client,db):
 global VESSEL_NAMES
 finished={r[0] for r in db.execute("SELECT number FROM specimens WHERE status IN ('DONE','ERROR')")}
 if len(finished)>5 and (STATE/'targets.json').exists():
  plan=json.loads((STATE/'targets.json').read_text(encoding='utf-8'))
  previous=json.loads((STATE/'status.json').read_text(encoding='utf-8-sig'));STATUS.update(previous)
  names=json.loads((STATE/'vessel_names.json').read_text(encoding='utf-8'))
  VESSEL_NAMES={str(r.get('vesselCode',''))[:4]:r for r in (names.get('response') or {}).get('data',[]) if isinstance(r,dict)}
  persist(status='running',phase='BPT' if set(plan['HJNC'])<=finished else 'HJNC',pid=os.getpid(),resumed_at=stamp(),resume_note='CSV 저장을 별도 작업으로 분리. 완료 번호 재조회하지 않음.')
  notify('DGT 명세 조회 재개\nCSV 저장을 별도 작업으로 분리했습니다.\n완료 '+format(len(finished),',')+'개는 재조회하지 않고 남은 번호부터 이어갑니다.')
  schedule_export()
 else:plan=prepare(client,db);persist(status='running',phase='HJNC')
 thread=threading.Thread(target=reporter,args=(plan,),daemon=True);thread.start()
 try:
  for group in ['HJNC','BPT']:
   finished={r[0] for r in db.execute("SELECT number FROM specimens WHERE status IN ('DONE','ERROR')")}
   if set(plan[group])<=finished:continue
   persist(phase=group);notify(progress_text(plan));since_export=0
   for n in plan[group]:
    stopped();r=db.execute('SELECT status FROM specimens WHERE number=?',(n,)).fetchone()
    if r and r[0] in ('DONE','ERROR'):continue
    collect(client,n,db);since_export+=1
    if since_export%20==0:progress_text(plan)
    if since_export>=1000:schedule_export();since_export=0
   schedule_export();notify(f'DGT {group} 명세 조회 완료\nCSV는 별도 작업으로 반영 중입니다.\n'+('BPT 명세 조회를 이어갑니다.' if group=='HJNC' else '최종 저장 검증을 진행합니다.'))
  persist(phase='final_validation');saved=export(plan);verified=final_verify(plan,saved)
  REPORT_END.set();thread.join(timeout=15)
  sent=notify(f'DGT 명세 조회·저장 검증 완료\nHJNC {verified["DGT_HJNC.csv"]:,}행\nBPT {verified["DGT_BPT.csv"]:,}행\nERROR {verified["ERROR.csv"]:,}행(기간불일치 포함)\n코드·설명·원문 보존 완료\n60초 후 노트북 종료')
  persist(status='completed',phase='shutdown_pending',final_saved=verified,final_kakao_sent=sent,finished_at=stamp())
  subprocess.run(['shutdown.exe','/s','/t','60','/c','DGT HJNC 및 BPT 명세 조회와 CSV 저장 검증 완료'],check=True)
  persist(status='completed_shutdown_scheduled',shutdown_seconds=60)
 finally:REPORT_END.set()
def main():
 global EXPORT_WORKER,VESSEL_NAMES
 parser=argparse.ArgumentParser();parser.add_argument('--prepare',action='store_true');parser.add_argument('--run',action='store_true');parser.add_argument('--export-only',action='store_true');args=parser.parse_args()
 if not args.prepare and not args.run and not args.export_only:parser.error('--prepare 또는 --run 필요')
 STATE.mkdir(parents=True,exist_ok=True);OUT.mkdir(parents=True,exist_ok=True)
 if args.export_only:
  EXPORT_WORKER=True;plan=json.loads((STATE/'targets.json').read_text(encoding='utf-8'))
  names=json.loads((STATE/'vessel_names.json').read_text(encoding='utf-8'))
  VESSEL_NAMES={str(r.get('vesselCode',''))[:4]:r for r in (names.get('response') or {}).get('data',[]) if isinstance(r,dict)}
  export(plan);return
 import msvcrt
 lock=(STATE/'job.lock').open('a+b');lock.seek(0);lock.write(b'0');lock.flush();lock.seek(0)
 try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
 except OSError:raise RuntimeError('이미 실행 중인 명세 조회가 있습니다.')
 db=dbconnect();client=Client()
 try:
  ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
  if args.prepare:
   plan=prepare(client,db);persist(status='prepared',phase='waiting_for_main_run')
   print(json.dumps({k:v for k,v in STATUS.items() if k!='vessel_names'},ensure_ascii=True),flush=True)
  else:run(client,db)
 except InterruptedError:
  REPORT_END.set();persist(status='paused_by_user',phase='stopped');notify('DGT 명세 조회를 사용자 요청으로 중단했습니다. 저장된 원문과 체크포인트를 보존합니다. 노트북을 종료하지 않습니다.')
 except Exception as e:
  REPORT_END.set();persist(status='failed',phase='failed',error=str(e));notify('DGT 명세 관리 오류로 중단했습니다. 체크포인트 보존, 노트북 종료하지 않음. 오류: '+str(e)[:90]);raise
 finally:
  ctypes.windll.kernel32.SetThreadExecutionState(0x80000000);client.pool.shutdown(wait=True);db.close();lock.close()
if __name__=='__main__':main()
