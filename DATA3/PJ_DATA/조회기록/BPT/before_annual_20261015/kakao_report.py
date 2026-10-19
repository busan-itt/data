"""본인 카카오톡 알림 연결 및 정각·30분 보고. Windows 사용자별 암호화 저장."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
import csv, ctypes, html, json, secrets, sqlite3, sys, time
import urllib.parse as U
import urllib.request as R
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from ctypes import wintypes as W

BASE=Path(__file__).resolve().parent
STATE=BASE/'crawl_state';STATE.mkdir(exist_ok=True)
VAULT=STATE/'kakao_credentials.dpapi'
KST=timezone(timedelta(hours=9))
REDIRECT='http://localhost:8765/callback'
LINK='https://info.bptc.co.kr'

class Blob(ctypes.Structure):
    _fields_=[('cbData',W.DWORD),('pbData',ctypes.POINTER(ctypes.c_ubyte))]

def crypt(data, decrypt=False):
    buf=ctypes.create_string_buffer(data)
    source=Blob(len(data),ctypes.cast(buf,ctypes.POINTER(ctypes.c_ubyte)));out=Blob()
    lib=ctypes.WinDLL('crypt32',use_last_error=True)
    if decrypt:
        fn=lib.CryptUnprotectData;fn.argtypes=[ctypes.POINTER(Blob),ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,W.DWORD,ctypes.POINTER(Blob)]
    else:
        fn=lib.CryptProtectData;fn.argtypes=[ctypes.POINTER(Blob),W.LPCWSTR,ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,W.DWORD,ctypes.POINTER(Blob)]
    if not fn(ctypes.byref(source),None,None,None,None,1,ctypes.byref(out)):raise ctypes.WinError(ctypes.get_last_error())
    try:return ctypes.string_at(out.pbData,out.cbData)
    finally:
        kernel=ctypes.WinDLL('kernel32');kernel.LocalFree.argtypes=[ctypes.c_void_p];kernel.LocalFree(out.pbData)

def save_config(data):VAULT.write_bytes(crypt(json.dumps(data).encode()))
def load_config():return json.loads(crypt(VAULT.read_bytes(),True))

def api(url, values, token=None):
    request=R.Request(url,data=U.urlencode(values).encode(),headers={'Content-Type':'application/x-www-form-urlencoded;charset=utf-8',**({'Authorization':'Bearer '+token} if token else {})})
    try:
        with R.urlopen(request,timeout=20) as response:return json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            detail=json.loads(exc.read());code=detail.get('error_code',detail.get('code',detail.get('error','unknown')))
        except ValueError:code='unknown'
        hints={
            'KOE010':'클라이언트 시크릿이 없거나 올바르지 않습니다. 같은 REST API 키의 시크릿 값을 다시 복사하세요.',
            'KOE101':'REST API 키가 올바르지 않습니다. 앱 ID나 다른 종류의 키 대신 REST API 키 값을 입력하세요.',
            'KOE114':'로그인 때 사용한 REST API 키와 토큰 요청 키가 다릅니다. 첫 화면에서 두 값을 다시 입력해 로그인하세요.',
            'invalid_client':'같은 REST API 키 상세 화면의 키 값과 클라이언트 시크릿을 다시 입력하세요.'}
        hint=hints.get(str(code),'동의항목·제품 링크 도메인·키 설정을 확인하세요.')
        raise RuntimeError(f'카카오 HTTP {exc.code}, 오류 코드 {code}. {hint}') from None

def send(text):
    cfg=load_config()
    if cfg['expires_at']<time.time()+120:
        result=api('https://kauth.kakao.com/oauth/token',{'grant_type':'refresh_token','client_id':cfg['client_id'],'client_secret':cfg['client_secret'],'refresh_token':cfg['refresh_token']})
        cfg.update(result);cfg['expires_at']=time.time()+result['expires_in'];save_config(cfg)
    template={'object_type':'text','text':text[:200],'link':{'web_url':LINK,'mobile_web_url':LINK},'button_title':'BPT 조회 사이트'}
    result=api('https://kapi.kakao.com/v2/api/talk/memo/default/send',{'template_object':json.dumps(template,ensure_ascii=False)},cfg['access_token'])
    if result.get('result_code')!=0:raise RuntimeError('카카오 전송 성공 응답을 확인하지 못했습니다.')

def progress():
    job=json.loads((STATE/'feb_mar_job_status.json').read_text(encoding='utf-8'));month=job['active_month'];suffix=month.replace('-','_')
    monthly=json.loads((STATE/f'bpt_{suffix}_job_status.json').read_text(encoding='utf-8'))
    with (STATE/f'targets_{suffix}.csv').open(encoding='utf-8-sig',newline='') as f:
        reader=csv.reader(f);next(reader);ids={r[0] for r in reader}
    db=sqlite3.connect(f'file:{(STATE/"progress.sqlite3").as_posix()}?mode=ro',uri=True)
    rows=[r for r in db.execute("SELECT container_no,status,queried_at FROM results WHERE service='BPT'") if r[0] in ids];db.close()
    ok={'FOUND','FOUND_UNCLASSIFIED','NO_RESULT_RETURNED'};done=sum(r[1] in ok for r in rows);found=sum(r[1].startswith('FOUND') for r in rows);empty=sum(r[1]=='NO_RESULT_RETURNED' for r in rows);errors=len(rows)-done
    now=datetime.now(KST);recent=sorted(datetime.fromisoformat(r[2]) for r in rows if datetime.fromisoformat(r[2])>=now-timedelta(minutes=30))
    speed=(recent[-1]-recent[0]).total_seconds()/(len(recent)-1) if len(recent)>1 else None
    end=job['status']!='running';network=json.loads((STATE/'network_status.json').read_text(encoding='utf-8')).get('status','unknown')
    text=f'BPT {month} {now:%m/%d %H:%M}\n정상 {done:,}/{len(ids):,}({done/len(ids):.1%})\n기록 {found:,} / 결과없음 {empty:,} / 오류 {errors}'
    if speed:text+=f'\n평균 {speed:.2f}초 / 잔여 약 {(len(ids)-done)*speed/60:.0f}분'
    text+=f'\n상태 {monthly["status"]} / 연결 {network}\n완료 후 PC 켜 둠'
    return text,end

def watch():
    # 한 PC에서 알림 프로세스가 중복 실행되지 않도록 Windows 잠금 사용.
    import msvcrt
    lock=(STATE/'kakao_report.lock').open('a+b');lock.seek(0);lock.write(b'0');lock.flush();lock.seek(0)
    try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
    except OSError:return
    current=datetime.now(KST);last=f'{current:%Y%m%d%H}:{current.minute//30}';first=False
    while True:
        now=datetime.now(KST);slot=f'{now:%Y%m%d%H}:{now.minute//30}';terminal=False
        try:
            text,terminal=progress()
            if (first or now.minute in (0,30) or terminal) and slot!=last:
                send(text);last=slot;first=False
                (STATE/'kakao_report_status.json').write_text(json.dumps({'status':'completed' if terminal else 'running','last_sent_at':now.isoformat()},ensure_ascii=False),encoding='utf-8')
            if terminal:return
        except Exception as exc:
            (STATE/'kakao_report_status.json').write_text(json.dumps({'status':'send_error','updated_at':now.isoformat(),'error':str(exc)},ensure_ascii=False),encoding='utf-8')
            time.sleep(45)
        time.sleep(10)

def setup():
    nonce=secrets.token_urlsafe(32);oauth_state=secrets.token_urlsafe(32);pending={};connected=False
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def reply(self,text,status=200):
            body=('<meta charset="utf-8"><title>BPT 카카오톡 연결</title><main style="font:18px sans-serif;max-width:700px;margin:40px auto">'+text+'</main>').encode();self.send_response(status);self.send_header('Content-Type','text/html;charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Security-Policy',"default-src 'none'; style-src 'unsafe-inline'; form-action 'self' https://kauth.kakao.com; frame-ancestors 'none'");self.end_headers();self.wfile.write(body)
        def do_GET(self):
            nonlocal connected
            path=U.urlparse(self.path)
            if self.headers.get('Host') not in ('localhost:8765','127.0.0.1:8765'):return self.reply('잘못된 주소',403)
            if path.path=='/':
                return self.reply(f'<h1>BPT 카카오톡 연결</h1><p>카카오디벨로퍼스에서 새로 만든 BPT 알림용 REST API 키의 값을 입력하세요. 이 컴퓨터에 암호화해 저장합니다.</p><form method="post" action="/connect"><input type="hidden" name="nonce" value="{nonce}"><p>REST API 키<br><input name="client_id" type="password" required style="width:95%"></p><p>클라이언트 시크릿<br><input name="client_secret" type="password" required style="width:95%"></p><p>앱 → 제품 링크 관리 → 웹 도메인에 <b>{LINK}</b> 등록도 완료하세요.</p><button>카카오 로그인하고 보고 연결</button></form>')
            if path.path!='/callback':return self.reply('페이지 없음',404)
            q=U.parse_qs(path.query)
            if q.get('state',[''])[0]!=oauth_state:return self.reply('연결 확인값 불일치. 처음부터 다시 연결하세요.',403)
            if 'code' not in q:return self.reply('카카오 로그인 또는 메시지 전송 동의가 완료되지 않았습니다.',400)
            try:
                result=api('https://kauth.kakao.com/oauth/token',{'grant_type':'authorization_code','client_id':pending['client_id'],'client_secret':pending['client_secret'],'redirect_uri':REDIRECT,'code':q['code'][0]})
                if 'talk_message' not in result.get('scope','').split():raise RuntimeError('카카오톡 메시지 전송 동의가 필요합니다.')
                cfg={**pending,**result,'expires_at':time.time()+result['expires_in']};save_config(cfg)
                text,_=progress();send(text);connected=True
                self.reply('<h1>연결 및 첫 보고 전송 완료</h1><p>휴대폰 카카오톡 나와의 채팅을 확인하세요. 이후 정각·30분과 완료 시에 보고합니다.</p>')
            except Exception as exc:self.reply('<h2>연결 확인 필요</h2><p>'+html.escape(str(exc))+'</p><a href="/">다시 연결</a>',400)
        def do_POST(self):
            if self.headers.get('Host') not in ('localhost:8765','127.0.0.1:8765'):return self.reply('잘못된 주소',403)
            if self.path!='/connect':return self.reply('페이지 없음',404)
            size=int(self.headers.get('Content-Length','0'))
            if not 0<size<8192:return self.reply('입력 크기 오류',400)
            form=U.parse_qs(self.rfile.read(size).decode())
            if form.get('nonce',[''])[0]!=nonce:return self.reply('다시 연결 화면을 열어주세요.',403)
            pending.update(client_id=form.get('client_id',[''])[0].strip(),client_secret=form.get('client_secret',[''])[0].strip())
            if not all(pending.values()):return self.reply('두 값을 모두 입력하세요.',400)
            url='https://kauth.kakao.com/oauth/authorize?'+U.urlencode({'client_id':pending['client_id'],'redirect_uri':REDIRECT,'response_type':'code','scope':'talk_message','state':oauth_state})
            self.send_response(303);self.send_header('Location',url);self.end_headers()
    server=HTTPServer(('127.0.0.1',8765),Handler);server.timeout=1
    deadline=time.monotonic()+7200
    while not connected and time.monotonic()<deadline:server.handle_request()
    server.server_close()
    if connected:watch()

if __name__=='__main__':
    if '--test-crypto' in sys.argv:
        sample=b'local-test';assert crypt(crypt(sample),True)==sample;print('DPAPI roundtrip passed')
    elif '--watch' in sys.argv:watch()
    else:setup()
