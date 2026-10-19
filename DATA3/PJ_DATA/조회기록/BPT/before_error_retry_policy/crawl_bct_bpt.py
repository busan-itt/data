"""BCT/BPT 공개 화면의 현재 조회 결과 수집. Python 3.10+.

설치: python -m pip install playwright
      python -m playwright install chromium
실행: python crawl_bct_bpt.py --limit 10
전체: python crawl_bct_bpt.py --limit 0
실패 재조회: python crawl_bct_bpt.py --limit 10 --retry-errors
CSV 재출력만: python crawl_bct_bpt.py --export-only

입력 번호의 2025년 이용 이력이나 DGT와의 실제 이동을 확정하는 코드가 아니다.
현재 공식 조회 화면에서 반환되는 내용을 저장한다. API 주소는 추측하지 않는다.
"""
import argparse
import csv
import hashlib
import json
import random
import re
import sqlite3
import shutil
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parent
BCT_URL = 'https://info.bct2-4.com/infoservice/'
BPT_URL = ('https://info.bptc.co.kr/content/cg/frame/cntr_frame_cg_kr.jsp'
           '?p_id=CONT_CN_KR&snb_div=service&snb_num=1')
BCT_PREFIX = 'mainframe_vframeset_hframeset_bodyframe_workframe_ISU010_form'
BCT_INPUT = f'#{BCT_PREFIX}_div_work_divGroupBox_edt_cycCntrno_input'
BCT_FIND = f'#{BCT_PREFIX}_div_commBtn_btn_find'
BCT_WORK = f'#{BCT_PREFIX}_div_work'
NO_DATA = re.compile(r'(조회된|검색된|해당|조회|검색)\s*(자료|데이터|결과|정보).*?(없|존재하지)|'
                     r'(컨테이너|자료|데이터).*?(존재하지 않|찾을 수 없)|no\s+(data|records)', re.I)
FAIL_DATA = re.compile(r'(세션.*만료|로그인.*필요|로그인.*하세요|권한.*없|접근.*제한|'
                       r'시스템.*오류|서버.*오류|통신.*오류|error\s*code)', re.I)
SUCCESS = {'FOUND', 'FOUND_UNCLASSIFIED', 'NO_RESULT_RETURNED'}
KST = timezone(timedelta(hours=9))


def now():
    return datetime.now(KST).isoformat(timespec='seconds')


def load_ids(path):
    with path.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        column = next((c for c in ['컨테이너번호', 'cntrNo', 'container_no']
                       if c in (reader.fieldnames or [])), None)
        if column is None:
            raise ValueError('입력 CSV에 컨테이너번호 또는 cntrNo 열이 필요합니다.')
        numbers = sorted({r[column].strip().upper() for r in reader if r[column].strip()})
    bad = [n for n in numbers if not re.fullmatch(r'[A-Z]{4}\d{7}', n)]
    if bad:
        raise ValueError(f'번호 형식 오류 {len(bad)}개: {bad[:10]}')
    return numbers


def select_month_ids(numbers, annual_file, month):
    """조회일자 기준 해당 월의 TI/TL/TX 번호를 기존 잔여 목록에서 선택."""
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("월은 YYYY-MM 형식이어야 합니다.")
    parent = set(numbers)
    selected = set()
    with Path(annual_file).open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"cntrNo", "cntrClass", "SOURCE_DATE"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("DGT 연간 원본에 필요한 열이 없습니다.")
        for r in reader:
            if (r["SOURCE_DATE"].startswith(month + "-")
                    and r["cntrClass"] in {"TI", "TL", "TX"}):
                number = r["cntrNo"].strip().upper()
                if number in parent: selected.add(number)
    return sorted(selected)


def connect(path):
    db = sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('''CREATE TABLE IF NOT EXISTS results (
      container_no TEXT, service TEXT, terminal TEXT, status TEXT,
      queried_at TEXT, url TEXT, raw_file TEXT, details_json TEXT, error TEXT,
      PRIMARY KEY(container_no, service))''')
    db.commit()
    return db


def save(db, number, service, result):
    db.execute('INSERT OR REPLACE INTO results VALUES (?,?,?,?,?,?,?,?,?)',
               (number, service, result.get('terminal', service), result['status'],
                now(), result.get('url', ''), result.get('raw_file', ''),
                json.dumps(result.get('details', {}), ensure_ascii=False),
                result.get('error', '')))
    db.commit()


DETAIL_COLUMNS = [
    '이용부두', '조회상태설명', '모선항차', '선박코드', '입항년도', '입항차',
    '크기코드', '규격', '적재공컨구분', '적재공컨확인', '선사코드', '상태코드',
    '환적구분', '중량원문', '차량번호', '장치위치', '선내위치',
    '양하일시', '반입일시', '반출일시', '적하일시', '접안일시', '출항일시',
    '양하작업종료일시', '본선작업종료일시', '정보확인메모',
]


def flatten_lookup_fields(row):
    """기존 JSON 원문에서 표시 항목만 추출. 빈 값과 코드 의미는 추정하지 않음."""
    number, service, terminal, status, *_ = row
    out = {name: '' for name in DETAIL_COLUMNS}
    out['이용부두'] = {'BPT-G': '감만', 'BPT-S': '신선대', 'BCT': 'BCT'}.get(terminal, '미확인')
    out['조회상태설명'] = {
        'FOUND': '기록 확인', 'FOUND_UNCLASSIFIED': '기록 확인·부두 구분 미확인',
        'NO_RESULT_RETURNED': '현재 조회 결과 없음', 'RESPONSE_UNPARSED': '응답 해석 보류',
        'LOOKUP_ERROR': '조회 오류',
    }.get(status, status)
    detail = json.loads(row[7])
    if status not in ('FOUND', 'FOUND_UNCLASSIFIED'):
        out['적재공컨확인'] = '조회 결과 없음' if status == 'NO_RESULT_RETURNED' else '미확인'
        return out
    if service == 'BPT':
        values = {}
        for table in detail.get('tables', []):
            for cells in table:
                if len(cells) % 2:
                    continue
                for i in range(0, len(cells), 2):
                    label = re.sub(r'\s+', '', cells[i])
                    value = cells[i + 1].strip()
                    # 최초에 나타나는 기본정보 표 항목을 사용; 요금 입력란은 사용하지 않음.
                    if label and label not in values:
                        values[label] = value
        mapping = {
            '선박코드': '선박코드', '입항년도': '입항년도', '입항차': '입항차',
            '크기코드': '크기', '규격': '규격', '선사코드': '선사코드',
            '상태코드': '상태', '환적구분': 'T/S', '중량원문': '무게',
            '차량번호': '차량번호', '장치위치': '장치위치', '선내위치': '선내위치',
            '양하일시': '양하일시', '반입일시': '반입일시', '반출일시': '반출일시',
            '적하일시': '적하일시', '접안일시': '접안일시', '출항일시': '출항일시',
            '양하작업종료일시': '양하작업종료일시', '본선작업종료일시': '본선작업종료일시',
        }
        for column, label in mapping.items():
            out[column] = values.get(label, '')
        if all(out[c] for c in ('선박코드', '입항년도', '입항차')):
            out['모선항차'] = '-'.join(out[c] for c in ('선박코드', '입항년도', '입항차'))
        for label in ('적재/공컨', '적재공컨', '공/적', '공적구분', 'F/E', 'FULL/EMPTY'):
            value = values.get(label, '')
            if value:
                out['적재공컨구분'] = value
                out['적재공컨확인'] = '원문에 명시'
                break
        if not out['적재공컨구분']:
            out['적재공컨확인'] = '원문에 별도 항목 없음'
        out['정보확인메모'] = '기본 선택 모선·차수의 기록. 크기는 원문 코드이며 피트 수로 환산하지 않음.'
    elif service == 'BCT':
        fields = detail.get('fields', {})
        def get(suffix):
            return next((v for k,v in fields.items() if k.endswith(suffix)), '')
        mapping = {
            '규격':'div_1_cmb_cycIso_comboedit_input',
            '크기코드':'div_1_cmb_cycSize_comboedit_input',
            '선사코드':'div_1_cmb_cycOper_comboedit_input',
            '적재공컨구분':'div_1_cmb_cycFmcd_comboedit_input',
            '환적구분':'div_1_cmb_cycTs_comboedit_input',
            '중량원문':'div_1_edt_cycWeight_input',
            '상태코드':'div_3_cmb_cycStatus_comboedit_input',
            '장치위치':'div_3_edt_cycLocation_input',
        }
        for column,suffix in mapping.items():out[column] = get(suffix)
        out['적재공컨확인'] = '원문에 명시' if out['적재공컨구분'] else '미확인'
        out['정보확인메모'] = 'BCT 상세 원문은 조회상세_JSON에 보존. 추출하지 못한 항목은 공란.'
    return out


RESULT_COLUMNS = [
    '컨테이너번호', '이용부두', '모선항차', '선박코드', '입항년도', '입항차',
    '크기코드', '규격', '적재공컨구분', '적재공컨확인', '선사코드',
    '컨테이너상태', '환적구분', '중량', '차량번호', '장치위치', '선내위치',
    '양하일시', '반입일시', '반출일시', '적하일시', '접안일시', '출항일시',
    '양하작업종료일시', '본선작업종료일시', '조회결과',
]
AUDIT_COLUMNS = [
    '컨테이너번호', '조회서비스', '터미널', '조회상태', '조회시각',
    '조회주소', '원문파일', '조회상세_JSON', '오류내용',
]


TIME_COLUMNS = [
    '양하일시', '반입일시', '반출일시', '적하일시', '접안일시', '출항일시',
    '양하작업종료일시', '본선작업종료일시',
]
PERIOD_START = datetime(2024, 11, 1)
PERIOD_END_EXCLUSIVE = datetime(2026, 3, 1)


def result_period_decision(row):
    """8개 작업 시각 중 하나라도 지정 기간에 해당하면 원래 행을 유지."""
    values = [str(row.get(c, '')).strip() for c in TIME_COLUMNS]
    values = [v for v in values if v]
    if not values: return 'no_times'
    invalid = False
    for value in values:
        try:
            dt = datetime.fromisoformat(value.replace('/', '-'))
            if dt.tzinfo is not None: dt = dt.astimezone(KST).replace(tzinfo=None)
        except (ValueError, TypeError):
            invalid = True
            continue
        if PERIOD_START <= dt < PERIOD_END_EXCLUSIVE:
            return 'included'
    return 'invalid_time' if invalid else 'outside_period'


def export(db, directory, state_directory=None):
    """출력 폴더에는 결과.csv·검증.csv만 생성하고 내부 파일은 별도 보관."""
    directory = Path(directory)
    state_directory = Path(state_directory or ROOT/'crawl_state')
    directory.mkdir(parents=True, exist_ok=True)
    scratch = state_directory/'export_work'
    scratch.mkdir(parents=True, exist_ok=True)
    raw_rows = db.execute('SELECT * FROM results ORDER BY container_no,service').fetchall()
    result_rows, audit_rows = [], []
    filter_counts = {}
    for r in raw_rows:
        flat = flatten_lookup_fields(r)
        row = {'컨테이너번호': r[0], **flat}
        row['컨테이너상태'] = flat['상태코드']
        row['중량'] = flat['중량원문']
        row['조회결과'] = flat['조회상태설명']
        decision = result_period_decision(row)
        filter_counts[decision] = filter_counts.get(decision, 0) + 1
        if decision == 'included':
            result_rows.append([row.get(c, '') for c in RESULT_COLUMNS])
        audit = list(r)
        if audit[6]:
            source = Path(audit[6])
            audit[6] = str(source if source.is_absolute() else state_directory/source)
        audit_rows.append(audit)
    for filename, columns, rows in (
        ('결과.csv', RESULT_COLUMNS, result_rows),
        ('검증.csv', AUDIT_COLUMNS, audit_rows),
    ):
        dest, tmp = directory/filename, scratch/(filename+'.tmp')
        with tmp.open('w', encoding='utf-8-sig', newline='') as f:
            writer=csv.writer(f);writer.writerow(columns);writer.writerows(rows)
        try:
            tmp.replace(dest)
        except PermissionError:
            # 파일명을 늘리지 않고 기존 CSV에 직접 쓴다. 잠겨 있으면 오류를 노출.
            shutil.copyfile(tmp, dest)
            try:tmp.unlink()
            except PermissionError:pass
    report=[]
    for service,terminal,status,count in db.execute(
        'SELECT service,terminal,status,COUNT(*) FROM results GROUP BY service,terminal,status'):
        report.append({'조회서비스':service,'터미널':terminal,'조회상태':status,'번호수':count})
    (state_directory/'summary.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    filter_report = {
        'period_start': '2024-11-01 00:00:00',
        'period_end_exclusive': '2026-03-01 00:00:00',
        'rule': 'any_valid_time_in_period',
        'counts': filter_counts, 'result_rows': len(result_rows), 'audit_rows': len(audit_rows),
    }
    (state_directory/'result_period_filter.json').write_text(
        json.dumps(filter_report,ensure_ascii=False,indent=2),encoding='utf-8')


def dialogs_for(page):
    messages = []
    def handle(dialog):
        messages.append(dialog.message)
        # 조회 안내 alert만 닫는다. 확인/동의/인증 대화상자는 수락하지 않는다.
        dialog.dismiss()
    page.on('dialog', handle)
    return messages


def raw_snapshot(directory, service, number, content):
    path = directory / 'raw' / service / f'{number}.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding='utf-8')
    return str(path.resolve())


def classify_terminal(tables):
    """부두명이 실제 결과의 터미널/부두 항목에 표시될 때만 분류."""
    found = set()
    for table in tables:
        for cells in table:
            for i, cell in enumerate(cells[:-1]):
                label = re.sub(r'\s+', '', cell)
                if re.fullmatch(r'(작업)?(터미널|부두)(명|구분)?[:：]?', label):
                    value = cells[i+1]
                    if '감만' in value: found.add('BPT-G')
                    if '신선대' in value: found.add('BPT-S')
    return next(iter(found)) if len(found) == 1 else 'BPT-UNKNOWN'


def bct_setup(page):
    response = page.goto(BCT_URL, wait_until='domcontentloaded', timeout=60000)
    if response and response.status >= 400:
        raise RuntimeError(f'BCT 접속 HTTP {response.status}')
    page.get_by_text('컨테이너조회', exact=True).wait_for(timeout=60000)
    # 현재 공식 화면의 두 공지 팝업. 자동 종료가 안 되면 사용자가 닫을 수 있다.
    for suffix in ['div_noti_default', 'div_noti_billing']:
        close = page.locator(f'[id$="introframe_form_{suffix}"]').get_by_text('X', exact=True)
        if close.count() and close.is_visible(): close.click()
    menu = page.get_by_text('컨테이너 정보 조회', exact=True)
    if menu.count() == 0 or not menu.first.is_visible():
        page.get_by_text('컨테이너조회', exact=True).click()
    page.get_by_text('컨테이너 정보 조회', exact=True).first.click()
    page.locator(BCT_INPUT).wait_for(state='visible', timeout=30000)


def bpt_setup(page):
    response = page.goto(BPT_URL, wait_until='domcontentloaded', timeout=60000)
    if response and response.status >= 400:
        raise RuntimeError(f'BPT 접속 HTTP {response.status}')
    page.frame_locator('#in').locator('input[name="v_cntr_no"]').wait_for(timeout=30000)


def bct_query(page, number, messages, directory):
    messages.clear()
    # 매번 입력/결과 영역을 초기화해서 이전 컨테이너 결과를 재사용하지 않는다.
    page.locator(f'#{BCT_PREFIX}_div_commBtn_btn_init').click()
    baseline = page.locator(BCT_WORK).locator('input').evaluate_all('''els =>
      Object.fromEntries(els.map(e=>[e.id,e.value]))''')
    page.locator(BCT_INPUT).fill(number)
    page.locator(BCT_INPUT).press('Tab')
    def matches(response):
        req = response.request
        encoded = (req.post_data or '') + ' ' + req.url
        return (urlparse(response.url).hostname == 'info.bct2-4.com'
                and req.resource_type in ('xhr', 'fetch')
                and number in unquote(encoded).upper())
    try:
        with page.expect_response(matches, timeout=30000) as pending:
            page.locator(BCT_FIND).click()
        response = pending.value
        if response.status >= 400:
            raise RuntimeError(f'BCT 조회 HTTP {response.status}')
        # 조회 서버 응답을 원문으로 보존. 빈 화면을 과거 이용 이력 없음으로 단정하지 않는다.
        try:
            response_text = response.text()
        except Exception as exc:
            response_text = f'[응답 원문 읽기 실패: {type(exc).__name__}: {exc}]'
        page.wait_for_timeout(1000)
    except Exception:
        if messages and NO_DATA.search(' '.join(messages)):
            detail = {'dialogs': messages.copy(), 'note': '공식 화면이 조회 결과 없음을 안내'}
            return {'status':'NO_RESULT_RETURNED','terminal':'BCT','url':page.url,
                    'details':detail,'raw_file':raw_snapshot(directory,'BCT',number,detail)}
        raise
    work = page.locator(BCT_WORK)
    fields = work.locator('input').evaluate_all('''els => Object.fromEntries(
      els.map(e => [e.id, e.value]).filter(([k,v]) => v !== ''))''')
    text = work.inner_text()
    detail = {'fields': fields, 'text': text, 'dialogs': messages.copy(),
              'response_url': response.url, 'http_status': response.status,
              'response_text': response_text}
    server_empty = False
    server_error = False
    try:
        root = ET.fromstring(response_text)
        ns = {'n': 'http://www.nexacroplatform.com/platform/dataset'}
        code = root.find("n:Parameters/n:Parameter[@id='ErrorCode']", ns)
        dataset = root.find("n:Dataset[@id='output1']", ns)
        rows = dataset.find('n:Rows', ns) if dataset is not None else None
        if code is not None:
            server_error = (code.text or '').strip() not in ('0', '')
            server_empty = ((code.text or '').strip() == '0' and rows is not None
                            and len(rows.findall('n:Row', ns)) == 0)
        detail['server_empty_output1'] = server_empty
    except ET.ParseError:
        pass
    if server_error or (messages and FAIL_DATA.search(' '.join(messages))):
        status = 'LOOKUP_ERROR'
    elif server_empty or NO_DATA.search(' '.join(messages)):
        status = 'NO_RESULT_RETURNED'
    else:
        substantive = {k:v for k,v in fields.items()
                       if 'divGroupBox' not in k and v != baseline.get(k, '')}
        status = 'FOUND' if substantive else 'RESPONSE_UNPARSED'
        if not substantive:
            detail['note'] = ('조회 응답은 정상 수신했지만 결과 필드가 비어 있습니다. '
                              '현재 결과 없음 또는 화면 처리 문제일 수 있어 확인 보류합니다.')
    # 판단 근거 메모까지 포함해 저장한다.
    raw = raw_snapshot(directory, 'BCT', number, detail)
    return {'status':status,'terminal':'BCT','url':page.url,'raw_file':raw,'details':detail,
            'error': ' '.join(messages) if status == 'LOOKUP_ERROR' else ''}


def bpt_query(page, number, messages, directory):
    messages.clear()
    # BPT는 통합 조회. 감만/신선대 주소를 추측해서 두 번 요청하지 않는다.
    # 공식 onblur 이벤트로 번호를 한 번 전송. 한 글자씩 입력하면
    # keyup과 blur가 중복 전송하여 대기 중 응답을 놓칠 수 있다.
    inp = page.frame_locator('#in')
    box = inp.locator('input[name="v_cntr_no"]')
    box.fill(number)
    with page.expect_response(lambda r: '/GetShip_servlet_cg_kr' in r.url, timeout=30000) as first:
        box.press('Tab')
    response = first.value
    response.body()
    if response.status >= 400: raise RuntimeError(f'BPT 모선 조회 HTTP {response.status}')
    page.wait_for_timeout(500)
    options = inp.locator('select[name="v_ship"]').evaluate('''s =>
      Array.from(s.options).map(o=>({value:o.value,text:o.text,selected:o.selected}))''')
    valid_options = [o for o in options if o['value'].strip()]
    if not valid_options:
        detail = {'ship_options':options, 'dialogs':messages.copy(),
                  'note':'공식 모선 목록 조회가 현재 선택 가능한 결과를 반환하지 않음'}
        return {'status':'NO_RESULT_RETURNED','terminal':'BPT-UNKNOWN','url':response.url,
                'details':detail,'raw_file':raw_snapshot(directory,'BPT',number,detail)}
    # 서버가 제공한 기본 모선과 차수를 유지해야 결과 조회가 허용된다.
    if not inp.locator('input[name="v_dup_no"]').input_value().strip():
        inp.locator('select[name="v_ship"]').select_option(valid_options[0]['value'])
    # 결과 iframe은 매번 빈 페이지로 초기화해 이전 결과와 혼동하지 않도록 한다.
    out = page.frame(name='out')
    if out is None: raise RuntimeError('BPT 결과 프레임을 찾지 못했습니다.')
    out.goto('https://info.bptc.co.kr/content/blank.htm')
    with page.expect_response(lambda r: '/content/cg/jsp/cntr_out_cg_kr.jsp' in r.url,
                              timeout=30000) as pending:
        inp.get_by_role('button', name='조회', exact=True).click()
    result_response = pending.value
    result_response.body()
    if result_response.status >= 400:
        raise RuntimeError(f'BPT 결과 조회 HTTP {result_response.status}')
    out = page.frame(name='out')
    out.wait_for_load_state('domcontentloaded')
    text = out.locator('body').inner_text()
    tables = out.locator('table').evaluate_all('''tables => tables.map(t =>
      Array.from(t.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim())))''')
    options = inp.locator('select[name="v_ship"]').evaluate('''s =>
      Array.from(s.options).map(o=>({value:o.value,text:o.text,selected:o.selected}))''')
    fields = out.locator('input,select,textarea').evaluate_all('''els =>
      els.filter(e=>e.type!=='hidden' && e.type!=='password').map(e=>
      ({name:e.name,id:e.id,value:e.value}))''')
    detail = {'tables':tables,'fields':fields,'text':text,'ship_options':options,
              'dialogs':messages.copy(), 'note':'현재 조회에서 기본 선택된 모선/차수 결과'}
    raw = raw_snapshot(directory, 'BPT', number, detail)
    terminal = classify_terminal(tables)
    # 실제 서버의 선택된 모선 라벨이 예: 'TYMA-2014-005-신선대'.
    # 항목에 부두가 없을 때 이 명시적인 라벨만 추가 근거로 사용한다.
    if terminal == 'BPT-UNKNOWN':
        selected = [o['text'].strip() for o in options if o['selected']]
        if len(selected) == 1:
            if re.search(r'-신선대\s*$', selected[0]): terminal = 'BPT-S'
            elif re.search(r'-감만\s*$', selected[0]): terminal = 'BPT-G'
    result_text = text + ' ' + ' '.join(messages)
    if FAIL_DATA.search(result_text): status = 'LOOKUP_ERROR'
    elif NO_DATA.search(result_text): status = 'NO_RESULT_RETURNED'
    else:
        shown = text + ' ' + ' '.join(f['value'] for f in fields)
        if number in re.sub(r'\s+', '', shown).upper():
            status = 'FOUND' if terminal != 'BPT-UNKNOWN' else 'FOUND_UNCLASSIFIED'
        else: status = 'RESPONSE_UNPARSED'
    return {'status':status,'terminal':terminal,'url':out.url,'raw_file':raw,'details':detail,
            'error':' '.join(messages) if status == 'LOOKUP_ERROR' else ''}



def internet_available():
    import socket
    for host, port in [('1.1.1.1', 443), ('www.msftconnecttest.com', 80)]:
        try:
            with socket.create_connection((host, port), timeout=1): return True
        except OSError: pass
    return False


def wait_for_internet(state_directory):
    """인터넷 단절은 조회 오류로 저장하지 않고 연결 복구까지 대기."""
    paused = False
    status_file = state_directory / 'network_status.json'
    while not internet_available():
        if not paused:
            print('NETWORK_PAUSED: 인터넷 연결 끊김. 조회를 멈추고 복구를 기다립니다.', flush=True)
            status_file.write_text(json.dumps({'status':'waiting_for_network','updated_at':now()}, ensure_ascii=False), encoding='utf-8')
        paused = True
        time.sleep(3)
    if paused:
        print('NETWORK_RESUMED: 연결 확인. 같은 컨테이너부터 조회를 재개합니다.', flush=True)
    status_file.write_text(json.dumps({'status':'online','updated_at':now()}, ensure_ascii=False), encoding='utf-8')
    return paused


def network_safe_call(operation, state_directory, on_reconnect=None):
    while True:
        reconnected = wait_for_internet(state_directory)
        try:
            if reconnected and on_reconnect: on_reconnect()
            result = operation()
            if isinstance(result, dict) and result.get('status') == 'LOOKUP_ERROR' and not internet_available():
                raise RuntimeError('ERR_INTERNET_DISCONNECTED')
            return result
        except Exception as exc:
            disconnected = 'ERR_INTERNET_DISCONNECTED' in str(exc) or not internet_available()
            if not disconnected: raise
            print('NETWORK_PAUSED: 통신 중 연결 단절. 현재 번호는 오류로 저장하지 않습니다.', flush=True)
            wait_for_internet(state_directory)
            if on_reconnect:
                network_safe_call(on_reconnect, state_directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--input', type=Path, default=ROOT/'DGT_remaining_130522.csv')
    parser.add_argument('--output', type=Path, default=ROOT/'crawl_results')
    parser.add_argument('--state-dir', type=Path, default=ROOT/'crawl_state')
    parser.add_argument('--month', help='DGT 조회일자 기준 대상 월: YYYY-MM')
    parser.add_argument('--annual-file', type=Path, default=Path('C:/work/PJ4/DATA2/DGT_2025_anmual.csv'))
    parser.add_argument('--limit', type=int, default=10, help='입력 번호 수, 0이면 전체')
    parser.add_argument('--service', choices=['all','BCT','BPT'], default='all')
    parser.add_argument('--delay-min', type=float, default=0.5, help='조회 사이 무작위 대기 시간 하한(초)')
    parser.add_argument('--delay-max', type=float, default=1.2, help='조회 사이 무작위 대기 시간 상한(초)')
    parser.add_argument('--export-every', type=int, default=10, help='CSV 전체 갱신 간격(처리 번호 수)')
    parser.add_argument('--retry-errors', action='store_true')
    parser.add_argument('--export-only', action='store_true')
    parser.add_argument('--export-dir', type=Path, help='기존 체크포인트를 별도 폴더에 CSV로 출력')
    args = parser.parse_args()
    if args.limit < 0 or args.export_every < 1 or not (0 < args.delay_min <= args.delay_max < float('inf')):
        parser.error('limit>=0, 0<delay-min<=delay-max이며 대기 시간은 유한해야 합니다.')
    args.output.mkdir(parents=True, exist_ok=True)
    args.state_dir.mkdir(parents=True, exist_ok=True)
    db = connect(args.state_dir/'progress.sqlite3')
    if args.export_only:
        export(db,args.export_dir or args.output,args.state_dir); db.close(); return
    numbers = load_ids(args.input)
    # 결과 폴더가 다른 입력 파일의 체크포인트와 섞이는 것을 방지.
    fingerprint = hashlib.sha256('\n'.join(numbers).encode()).hexdigest()
    manifest = args.state_dir/'input_manifest.json'
    if manifest.exists() and json.loads(manifest.read_text(encoding='utf-8'))['sha256'] != fingerprint:
        raise ValueError('다른 입력 목록의 결과 폴더입니다. --output에 새 폴더를 지정하세요.')
    manifest.write_text(json.dumps({'sha256':fingerprint,'count':len(numbers),
                                   'scope':'current_default_lookup','created_at':now()},indent=2),encoding='utf-8')
    if args.month:
        numbers = select_month_ids(numbers, args.annual_file, args.month)
        target_file = args.state_dir / ("targets_" + args.month.replace("-", "_") + ".csv")
        with target_file.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f); writer.writerow(["컨테이너번호"])
            writer.writerows([[n] for n in numbers])
        if not numbers:
            raise ValueError(f"{args.month}에 해당하는 조회 대상이 없습니다.")
        print(f"대상 월 {args.month}: {len(numbers):,}개 번호. 기존 성공 조회는 유지합니다.")
    if args.limit: numbers = numbers[:args.limit]
    from playwright.sync_api import sync_playwright
    services = ['BCT','BPT'] if args.service == 'all' else [args.service]
    print(f'입력 {len(numbers):,}개 번호 / 서비스 {services} / 무작위 대기 {args.delay_min}~{args.delay_max}초')
    print('BPT 부두명이 결과에 없으면 부두 구분 미확인으로 저장합니다.')
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(viewport={'width':1600,'height':1000})
            for service in services:
                page = context.new_page(); page.set_default_timeout(30000)
                messages = dialogs_for(page)
                setup = bct_setup if service=='BCT' else bpt_setup
                query = bct_query if service=='BCT' else bpt_query
                network_safe_call(lambda: setup(page), args.state_dir)
                failures = 0
                for i,number in enumerate(numbers,1):
                    old = db.execute('SELECT status FROM results WHERE container_no=? AND service=?',
                                     (number,service)).fetchone()
                    if old and (old[0] in SUCCESS or not args.retry_errors): continue
                    try:
                        result = network_safe_call(lambda: query(page,number,messages,args.state_dir),
                                                   args.state_dir, lambda: setup(page))
                    except Exception as exc:
                        result = {'status':'LOOKUP_ERROR','terminal':service if service=='BCT' else 'BPT-UNKNOWN',
                                  'url':page.url,'error':f'{type(exc).__name__}: {exc}',
                                  'details':{'dialogs':messages.copy()}}
                    save(db,number,service,result)
                    print(f'{service} {i:,}/{len(numbers):,} {number}: {result["status"]} ({result["terminal"]})')
                    # 정상 응답의 해석 보류는 연결 실패와 구분해 다음 번호로 진행.
                    # 보류 상태는 CSV에 그대로 남고 --retry-errors로 재조회할 수 있다.
                    failures = failures+1 if result['status'] == 'LOOKUP_ERROR' else 0
                    if i % args.export_every == 0: export(db,args.output,args.state_dir)
                    if failures >= 3:
                        raise RuntimeError('3회 연속 접속/조회 오류입니다. 검증.csv와 실행 로그를 확인하세요.')
                    time.sleep(random.uniform(args.delay_min, args.delay_max))
                page.close()
            browser.close()
    except KeyboardInterrupt:
        print('중단되었습니다. 저장된 체크포인트에서 이어서 실행할 수 있습니다.')
    finally:
        export(db,args.output,args.state_dir)
        db.close()
        print(f'CSV 저장 위치: {args.output.resolve()}')


if __name__ == '__main__':
    main()
