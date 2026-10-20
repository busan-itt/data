"""DGT 반출입 목록의 환적 번호 중 HJNC/BPT 조회 번호를 제외한 명세 수집.

인자 없이 실행하면 오프라인 대상 준비만 수행합니다. --run만 실제 조회합니다.
기존 dgt_spec_job.py의 공개 명세 요청 및 원문 변환을 재사용합니다.
"""
import argparse
import contextlib
import csv
import gzip
import hashlib
import importlib.util
import json
import os
import random
import re
import socket
import sqlite3
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = Path('C:/work/DATA')
STATE_NAME = 'DGT_자부두환적'
TS_CLASSES = {'TI', 'TX', 'TL'}
KST = timezone(timedelta(hours=9))
LO, HI = datetime(2024, 11, 1), datetime(2026, 3, 1)
FILES = {'SELF': 'DGT_SELF_TS.csv', 'REVIEW': '검토.csv',
         'OTHER': '비해당.csv', 'ERROR': 'ERROR.csv'}
HEAD = ['컨테이너번호', 'DGT_방문회차', '조회상태', '시도횟수', '판정', '판정이유',
        '원본과최소시각차이_일', '명세반입반출간격_시간', '주기연결주의', '조회시각',
        '원문파일', '상세오류', '명세.cntrClass', '명세.in.mode', '명세.out.mode',
        '명세.in.vvd', '명세.out.vvd', '명세.in.inDate', '명세.out.outDate']
csv.field_size_limit(2**30)
NOTIFY_LOCK = threading.Lock()


def stamp():
    return datetime.now(KST).isoformat(timespec='seconds')


def norm(value):
    return re.sub('[^A-Z0-9]', '', str(value).upper())


def write_json(path, obj):
    atomic(path, json.dumps(obj, ensure_ascii=False, indent=2).encode('utf-8'))


def atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_bytes(data)
    replace(tmp, path)


def replace(src, dest):
    for _ in range(30):
        try:
            os.replace(src, dest)
            return
        except PermissionError:
            time.sleep(.2)
    raise RuntimeError('파일이 열려 있어 저장하지 못했습니다: ' + str(dest))


def helper(base):
    path = base / '크롤링 코드/DGT/dgt_spec_job.py'
    spec = importlib.util.spec_from_file_location('dgt_self_ts_helpers', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # 기존 main/run/notify/shutdown은 호출하지 않습니다.
    return module


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''):
            digest.update(block)
    return {'path': str(path), 'bytes': path.stat().st_size, 'sha256': digest.hexdigest()}


def connect(path, readonly=False):
    if readonly:
        return sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=30)
    db = sqlite3.connect(path, timeout=30)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    return db


@contextlib.contextmanager
def job_lock(state):
    import msvcrt
    state.mkdir(parents=True, exist_ok=True)
    with (state / 'job.lock').open('a+b') as f:
        f.seek(0)
        f.write(b'0')
        f.flush()
        f.seek(0)
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            raise RuntimeError('이 자부두 조회 작업이 이미 실행 중입니다.') from None
        yield


def prepare(base, state):
    source = base / 'PJ_DATA/dgt.csv'
    old_plan = base / 'PJ_DATA/조회기록/DGT_명세_20261018/targets.json'
    inputs = [fingerprint(source), fingerprint(old_plan)]
    summary_file = state / '대상준비.json'
    target_db = state / 'targets.sqlite3'
    if summary_file.exists() and target_db.exists():
        summary = json.loads(summary_file.read_text(encoding='utf-8'))
        if summary['inputs'] != inputs:
            raise RuntimeError('입력이 변경됐습니다. 기존 작업을 덮어쓰지 말고 별도 --job-name을 지정하세요.')
        return summary
    if target_db.exists():
        raise RuntimeError('미완성 대상 DB가 있습니다. 별도 --job-name으로 준비하세요.')
    previous = json.loads(old_plan.read_text(encoding='utf-8-sig'))
    excluded = {norm(n) for group in ('HJNC', 'BPT') for n in previous[group]}
    db = sqlite3.connect(target_db)
    try:
        db.executescript('''
        CREATE TABLE excluded(number TEXT PRIMARY KEY);
        CREATE TABLE source_rows(number TEXT,row_no INTEGER,in_date TEXT,out_date TEXT,
          class TEXT,in_mode TEXT,out_mode TEXT,vvd TEXT);
        CREATE TABLE targets(number TEXT PRIMARY KEY);
        ''')
        db.executemany('INSERT INTO excluded VALUES (?)', ((n,) for n in excluded))
        source_ids, ts_ids, class_rows, modes = set(), set(), Counter(), Counter()
        source_count = ts_count = invalid = 0
        batch = []
        with source.open(encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            required = {'컨테이너번호', 'Class', '반입시간', '반출시간', '반입', '반출', '모선항차'}
            if not required <= set(reader.fieldnames or []):
                raise ValueError('dgt.csv 필수 열 누락')
            for line, row in enumerate(reader, 2):
                source_count += 1
                number = norm(row['컨테이너번호'])
                source_ids.add(number)
                cls = row['Class'].strip().upper()
                if cls not in TS_CLASSES:
                    continue
                ts_count += 1
                class_rows[cls] += 1
                if not re.fullmatch(r'[A-Z]{4}[0-9]{7}', number):
                    invalid += 1
                    continue
                ts_ids.add(number)
                if number in excluded:
                    continue
                modes[row['반입'] + '→' + row['반출']] += 1
                batch.append((number, line, row['반입시간'], row['반출시간'], cls,
                              row['반입'], row['반출'], row['모선항차']))
                if len(batch) >= 5000:
                    db.executemany('INSERT INTO source_rows VALUES (?,?,?,?,?,?,?,?)', batch)
                    batch.clear()
        db.executemany('INSERT INTO source_rows VALUES (?,?,?,?,?,?,?,?)', batch)
        db.execute('CREATE INDEX source_number ON source_rows(number)')
        db.execute('INSERT INTO targets SELECT DISTINCT number FROM source_rows')
        assert db.execute('SELECT COUNT(*) FROM targets JOIN excluded USING(number)').fetchone()[0] == 0
        total = db.execute('SELECT COUNT(*) FROM targets').fetchone()[0]
        db.commit()
        summary = {'created_at': stamp(), 'status': 'prepared_not_running', 'inputs': inputs,
                   'source_rows': source_count, 'source_unique': len(source_ids),
                   'transshipment_classes': sorted(TS_CLASSES), 'transshipment_rows': ts_count,
                   'transshipment_unique': len(ts_ids), 'excluded_unique': len(excluded),
                   'excluded_in_ts': len(ts_ids & excluded), 'target_unique': total,
                   'invalid_ts_rows': invalid, 'class_rows': dict(class_rows),
                   'remaining_source_modes': dict(modes),
                   'exclusion_rule': '기존 조회 HJNC/BPT targets.json 전체 번호 합집합. 오류·기간불일치도 제외.',
                   'scope_note': '원본 환적 표시 번호의 다른 방문 주기를 발견할 수 있음. 원본 도로 회차와 동일 회차라고 해석하지 않음.'}
        write_json(summary_file, summary)
        return summary
    finally:
        db.close()


def in_window(value, h):
    parsed = h.dt(value)
    return parsed if parsed is not None and LO <= parsed < HI else None


def classify(record, references, h):
    """한 명세 주기 내 S→S만 평가. 별개 주기를 이어 붙이지 않습니다."""
    i, o = record.get('in') or {}, record.get('out') or {}
    if i.get('mode') != 'S' or o.get('mode') != 'S':
        return 'OTHER', '선박 반입·선박 반출 회차가 아님', None, None
    a, b = in_window(i.get('inDate'), h), in_window(o.get('outDate'), h)
    if a is None or b is None:
        return 'REVIEW', '분석기간 내 양쪽 작업시각 미확인', None, None
    hours = (b-a).total_seconds()/3600
    if b <= a:
        return 'REVIEW', '반출시각이 반입시각보다 늦지 않음', None, hours
    if not h.within(a, b):
        return 'REVIEW', '명세 반입·반출 간격 3개월 초과', None, hours
    refs = [t for value in references if (t := in_window(value, h)) is not None]
    if not refs:
        return 'REVIEW', '원본 환적 행에 기간 내 기준시각 없음', None, hours
    pairs = [(abs((x-y).total_seconds()), x, y) for x in (a, b) for y in refs]
    gap, x, y = min(pairs)
    gap_days = round(gap/86400, 6)
    if not h.within(x, y):
        return 'REVIEW', '원본 환적 행과 3개월 초과', gap_days, hours
    vin, vout = str(i.get('vvd') or ''), str(o.get('vvd') or '')
    if not re.fullmatch(r'[A-Z0-9]{4}-.+', vin) or not re.fullmatch(r'[A-Z0-9]{4}-.+', vout):
        return 'REVIEW', '양하 또는 적하 모선항차 미확인', gap_days, hours
    if vin[:4] == vout[:4]:
        return 'REVIEW', '동일 모선 코드: 재적재·동일선 작업 검토', gap_days, hours
    if i.get('inRego') or o.get('outRego'):
        return 'REVIEW', '선박 운송방식과 차량번호가 함께 있어 검토 필요', gap_days, hours
    if str(record.get('cntrClass', '')).upper() not in TS_CLASSES:
        return 'REVIEW', '명세 Class의 환적 표시 미확인', gap_days, hours
    if record.get('stamp') != 'Loaded' or str(record.get('cancel') or '').upper() not in ('', 'N', 'NO', '0'):
        return 'REVIEW', '적하 완료 또는 취소 여부 확인 필요', gap_days, hours
    return 'SELF', '동일 명세 회차 S→S·서로 다른 모선·기간 및 완료 조건 충족', gap_days, hours


def init_progress(state):
    db = connect(state / 'progress.sqlite3')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS specimens(number TEXT PRIMARY KEY,status TEXT,attempts INTEGER,
      queried_at TEXT,raw_file TEXT,error TEXT,seconds REAL);
    CREATE TABLE IF NOT EXISTS output_rows(number TEXT,point TEXT,dest TEXT,data TEXT,
      PRIMARY KEY(number,point));
    ''')
    return db


def status(state, **values):
    path = state / 'status.json'
    previous = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    previous.update(values)
    previous['updated_at'] = stamp()
    write_json(path, previous)


def stopped(state):
    if (state / 'stop_requested.json').exists():
        raise InterruptedError('사용자 중단 요청')


def pause(state, seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        stopped(state)
        time.sleep(min(.5, max(0, end-time.monotonic())))


def online():
    for host in ('www.microsoft.com', 'www.google.com'):
        try:
            with socket.create_connection((host, 443), timeout=4):
                return True
        except OSError:
            pass
    return False


def wait_network(state):
    if online():
        return
    status(state, status='network_wait')
    write_json(state/'network_status.json', {'status': 'waiting', 'wait_started_at': stamp()})
    while not online():
        pause(state, random.uniform(30, 60))
    status(state, status='running')
    write_json(state/'network_status.json', {'status': 'restored', 'restored_at': stamp()})


def raw_path(state, number):
    return state / 'raw' / number[:2] / (number + '.json.gz')


def save_doc(state, doc):
    atomic(raw_path(state, doc['container_no']), gzip.compress(
        json.dumps(doc, ensure_ascii=False).encode('utf-8'), compresslevel=4))


def collect(client, number, refs, state, h):
    path = raw_path(state, number)
    if path.exists():
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            doc = json.load(f)
    else:
        doc = {'container_no': number, 'main': None, 'steps': {}, 'step_errors': {}, 'attempts': 0}
    if doc.get('collection_complete'):
        return doc  # 원문 저장 직후 중단돼도 성공 번호를 다시 요청하지 않음.
    while doc['attempts'] < 3:
        stopped(state)
        wait_network(state)
        doc['attempts'] += 1
        doc['queried_at'] = stamp()
        save_doc(state, doc)  # 요청 전 시도 횟수를 기록해 중단·재개 때 3회 제한 유지.
        issue = ''
        try:
            if not client.headers:
                client.refresh()
            if doc['main'] is None:
                response = client.post('/DGT/container/containers', {
                    'filter': {'cntrNos': [number], 'stamp': ['B', 'Y', 'O']}})['response']
                records = response.get('containers') or []
                if int(response.get('searchedCount', -1)) != len(records):
                    raise ValueError('기본 명세 응답 건수 불일치')
                points = []
                for r in records:
                    if norm(r.get('cntrNo')) != number or r.get('point') is None:
                        raise ValueError('번호 또는 방문 주기 불일치')
                    points.append(str(r['point']))
                if len(points) != len(set(points)):
                    raise ValueError('같은 방문 주기의 중복 응답')
                doc['main'] = response
                save_doc(state, doc)
            for record in doc['main'].get('containers') or []:
                dest, _, _, _ = classify(record, refs, h)
                # 조건 충족 회차만 기존 명세조회와 같은 상세 항목을 추가 수집.
                if dest != 'SELF':
                    continue
                for key, endpoint, body, _ in h.requests_for(number, record):
                    stopped(state)
                    if key in doc['steps']:
                        continue
                    try:
                        answer = client.post(endpoint, body, False, True)
                        doc['steps'][key] = {'path': endpoint, 'request': body, **answer}
                        doc['step_errors'].pop(key, None)
                        save_doc(state, doc)
                    except Exception as exc:
                        doc['step_errors'][key] = str(exc)
                        issue = str(exc)
            if doc['step_errors']:
                issue = issue or '상세 항목 오류'
        except Exception as exc:
            issue = str(exc)
        if issue and not online():
            doc['attempts'] -= 1  # 단절 대기는 조회 오류·시도 횟수에 포함하지 않음.
            save_doc(state, doc)
            client.headers = {}
            wait_network(state)
            continue
        doc['error'] = issue
        doc['collection_complete'] = not bool(issue)
        save_doc(state, doc)
        if not issue:
            return doc
        client.headers = {}
        pause(state, random.uniform(.7, 1.5))
    doc['error'] = doc.get('error') or '3회 시도 후 미해결'
    return doc


def store_result(db, state, number, refs, doc, seconds, h):
    records = (doc.get('main') or {}).get('containers') or []
    rows = []
    for r in records or [None]:
        point = str(r['point']) if r else ''
        dest, reason, gap, hours = classify(r, refs, h) if r else ('REVIEW', '명세 결과 없음', None, None)
        failures = {k: v for k, v in doc['step_errors'].items() if k.startswith(point + ':')}
        if failures or (doc.get('error') and (not records or not doc['step_errors'])):
            dest = 'ERROR'
            reason = '3회 시도 후 조회 오류. 원문에 성공 항목 보존.'
        row = {'컨테이너번호': number, 'DGT_방문회차': point,
               '조회상태': 'ERROR' if dest == 'ERROR' else 'DONE', '시도횟수': doc['attempts'],
               '판정': dest, '판정이유': reason, '원본과최소시각차이_일': gap,
               '명세반입반출간격_시간': hours, '조회시각': doc.get('queried_at', ''),
               '원문파일': str(raw_path(state, number)),
               '주기연결주의': '원본 번호에서 발견한 명세 회차. 원본 도로 회차와 동일 회차 확정 아님.',
               '상세오류': json.dumps(failures, ensure_ascii=False) if failures else doc.get('error', '')}
        if r:
            h.flatten(r, '명세', row)
            for key, value in doc['steps'].items():
                if key.startswith(point + ':'):
                    h.flatten(value, '상세.' + key.split(':', 1)[1], row)
        rows.append((number, point, dest, json.dumps(row, ensure_ascii=False)))
    # 출력 행과 체크포인트를 같은 트랜잭션으로 저장해 재개 시 누락을 방지합니다.
    with db:
        db.execute('DELETE FROM output_rows WHERE number=?', (number,))
        db.executemany('INSERT INTO output_rows VALUES (?,?,?,?)', rows)
        db.execute('INSERT OR REPLACE INTO specimens VALUES (?,?,?,?,?,?,?)',
                   (number, 'ERROR' if doc.get('error') else 'DONE', doc['attempts'],
                    doc.get('queried_at', ''), str(raw_path(state, number)), doc.get('error', ''), seconds))


def export(state, out, expected=None):
    out.mkdir(parents=True, exist_ok=True)
    db = connect(state/'progress.sqlite3', True)
    try:
        fields = set(HEAD)
        for data, in db.execute('SELECT data FROM output_rows'):
            fields.update(json.loads(data))
        headers = HEAD + sorted(fields - set(HEAD))
        counts, unique = Counter(), {k: set() for k in FILES}
        with contextlib.ExitStack() as stack:
            writers = {}
            for key, name in FILES.items():
                stream = stack.enter_context((out/(name+'.tmp')).open('w', encoding='utf-8-sig', newline=''))
                writers[key] = csv.DictWriter(stream, fieldnames=headers)
                writers[key].writeheader()
            for number, dest, data in db.execute('SELECT number,dest,data FROM output_rows ORDER BY number,point'):
                writers[dest].writerow(json.loads(data))
                counts[dest] += 1
                unique[dest].add(number)
        for name in FILES.values():
            replace(out/(name+'.tmp'), out/name)
        represented = set().union(*unique.values())
        finished = {r[0] for r in db.execute('SELECT number FROM specimens')}
        if represented != finished:
            raise ValueError('CSV 결과에 체크포인트 번호 누락')
        if expected is not None and represented != expected:
            raise ValueError('최종 결과에 대상 번호 누락·혼입')
        result = {'saved_at': stamp(), 'rows': dict(counts),
                  'unique_ids': {k: len(v) for k, v in unique.items()},
                  'processed_unique': len(finished), 'target_complete': expected is not None,
                  'verified': True}
        write_json(out/'저장검증.json', result)
        status(state, last_export=result)
        return result
    finally:
        db.close()


def notify(base, state, text, enabled):
    if not enabled:
        return
    try:
        sys.path.insert(0, str(base/'크롤링 코드/BPT'))
        import kakao_report
        with NOTIFY_LOCK:
            kakao_report.send(text)
        write_json(state/'kakao_report_status.json', {'status': 'sent', 'sent_at': stamp(), 'text': text[:200]})
    except Exception as exc:
        write_json(state/'kakao_report_status.json', {'status': 'error', 'updated_at': stamp(), 'error': str(exc)})


def progress(db, total):
    done = db.execute('SELECT COUNT(*) FROM specimens').fetchone()[0]
    recent = db.execute('SELECT seconds FROM specimens ORDER BY queried_at DESC LIMIT 200').fetchall()
    speed = sum(r[0] for r in recent)/len(recent) if recent else None
    counts = dict(db.execute('SELECT dest,COUNT(*) FROM output_rows GROUP BY dest'))
    eta = datetime.now(KST)+timedelta(seconds=(total-done)*speed) if speed else None
    text = f'DGT 자부두 {done:,}/{total:,}\n조건충족 {counts.get("SELF",0):,}행 / 오류 {counts.get("ERROR",0):,}행'
    if eta:
        text += f'\n예상 완료 {eta:%m/%d %H:%M} (실측 추정)'
    return text, done, eta.isoformat(timespec='minutes') if eta else None


def reporter(base, state, total, enabled, end):
    """CSV 변환·연결 대기와 별도로 정각/30분 보고 시도. 인증정보는 기록하지 않음."""
    last = datetime.now(KST).strftime('%Y%m%d%H') + str(datetime.now(KST).minute//30)
    while not end.wait(10):
        slot = datetime.now(KST).strftime('%Y%m%d%H') + str(datetime.now(KST).minute//30)
        if slot == last:
            continue
        try:
            db = connect(state/'progress.sqlite3', True)
            try:
                text, _, _ = progress(db, total)
            finally:
                db.close()
            notify(base, state, text, enabled)
        except Exception as exc:
            write_json(state/'report_error.json', {'at': stamp(), 'error': str(exc)})
        last = slot


def run(base, state, out, summary, limit, kakao, h):
    db = init_progress(state)
    targets = connect(state/'targets.sqlite3', True)
    client = h.Client()
    total = summary['target_unique']
    count = 0
    report_end = threading.Event()
    report_thread = None
    try:
        status(state, status='running', pid=os.getpid(), total=total, phase='basic_then_detail')
        notify(base, state, f'DGT 자부두 명세 조회 시작\n대상 {total:,}개\n기존 HJNC/BPT 번호 제외\n0.7~1.5초 대기, 오류 총3회\n표본 실측 후 예상시간 보고', kakao)
        report_thread = threading.Thread(target=reporter,
            args=(base, state, total, kakao, report_end), daemon=True)
        report_thread.start()
        for number, in targets.execute('SELECT number FROM targets ORDER BY number'):
            stopped(state)
            if db.execute('SELECT 1 FROM specimens WHERE number=?', (number,)).fetchone():
                continue
            if limit is not None and count >= limit:
                break
            refs = [v for row in targets.execute('SELECT in_date,out_date FROM source_rows WHERE number=?', (number,)) for v in row]
            begin = time.monotonic()
            doc = collect(client, number, refs, state, h)
            store_result(db, state, number, refs, doc, time.monotonic()-begin + 1.1, h)
            pause(state, random.uniform(.7, 1.5))
            count += 1
            if count % 20 == 0:
                text, done, eta = progress(db, total)
                status(state, completed=done, estimated_finished_at=eta)
                print(text, flush=True)
                if count == 20:
                    notify(base, state, text, kakao)
            if count % 1000 == 0:
                export(state, out)
        done = db.execute('SELECT COUNT(*) FROM specimens').fetchone()[0]
        expected = {r[0] for r in targets.execute('SELECT number FROM targets')} if done == total else None
        result = export(state, out, expected)
        final = 'completed' if expected is not None else 'sample_completed'
        report_end.set()
        report_thread.join(timeout=25)
        status(state, status=final, completed=done, finished_at=stamp())
        notify(base, state, f'DGT 자부두 {final}\n{done:,}/{total:,}개 처리\nCSV 저장 검증 완료\n조건충족 {result["rows"].get("SELF",0):,}행\n노트북 전원 유지', kakao)
    except (InterruptedError, KeyboardInterrupt):
        report_end.set()
        export(state, out)
        status(state, status='paused_by_user')
        notify(base, state, 'DGT 자부두 조회 중단. 체크포인트와 CSV 저장 완료. 재실행 시 남은 번호부터 재개.', kakao)
    except Exception as exc:
        report_end.set()
        status(state, status='failed', error=str(exc))
        notify(base, state, 'DGT 자부두 관리 오류로 중단. 체크포인트 보존: '+str(exc)[:80], kakao)
        raise
    finally:
        report_end.set()
        if report_thread is not None:
            report_thread.join(timeout=25)
        client.pool.shutdown(wait=True)
        targets.close()
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--prepare', action='store_true', help='오프라인 대상 준비만 수행 (기본)')
    mode.add_argument('--run', action='store_true', help='실제 명세 조회 및 재개')
    mode.add_argument('--export-only', action='store_true', help='체크포인트를 CSV로 저장')
    parser.add_argument('--limit', type=int, help='이번 실행에서 새 번호 N개만 조회')
    parser.add_argument('--no-kakao', action='store_true')
    parser.add_argument('--base', type=Path, default=BASE)
    parser.add_argument('--job-name', default=STATE_NAME)
    args = parser.parse_args()
    if not re.fullmatch(r'[\w\-]+', args.job_name) or (args.limit is not None and args.limit < 1):
        parser.error('job-name 또는 limit 값이 올바르지 않습니다.')
    state = args.base/'PJ_DATA/조회기록'/args.job_name
    out = args.base/'PJ_DATA/조회결과'/args.job_name
    with job_lock(state):
        if args.export_only:
            print(json.dumps(export(state, out), ensure_ascii=False))
            return
        summary = prepare(args.base, state)
        if args.run:
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
            try:
                run(args.base, state, out, summary, args.limit, not args.no_kakao, helper(args.base))
            finally:
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
        else:
            status(state, status='prepared_not_running', total=summary['target_unique'])
            print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
