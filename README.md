# HJNC 2025 Gate In/Out 데이터

`DATA` 폴더에는 파일명에 `inout`이 포함된 CSV만 저장합니다. 모든 CSV는 Git LFS로 관리합니다.

- `HJNC_2025_MM_gate_inout_raw.csv`: API에서 수집한 월별 원본 데이터입니다.
- `HJNC_2025_MM_gate_inout_korean.csv`: 원본과 같은 행을 사용하고 컬럼명을 한글로 변환한 월별 데이터입니다.
- `HJNC_2025_gate_inout_korean_annual.csv`: 1월부터 12월까지의 한글 변환본을 월 순서대로 합친 연간 데이터입니다. 총 1,372,091건입니다.
- `HJNC_2025-01-18_gate_inout_recollected_raw.csv`, `HJNC_2025-08-24_gate_inout_recollected_raw.csv`: 최초 수집 오류 날짜를 다시 수집한 검증용 원본입니다. 해당 결과는 월별 원본과 한글 변환본에 반영되어 있습니다.

한글 변환본의 주요 컬럼 예시는 `컨테이너번호`, `반입시간`, `반출시간`, `차량번호`, `선사`, `조회일자`입니다.
