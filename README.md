# 부산 신항 환적 분석 데이터

**부산 신항의 선석 통합운영을 통한 환적 체류시간 저감** 프로젝트의 데이터 저장소입니다.

컨테이너 출입 이력, 자부두·타부두 환적 자료, 터미널별 선석배정 자료를 관리합니다.

## 데이터 구성

| 폴더 | 주요 내용 | 대상 |
| :--- | :--- | :--- |
| [부두별 연간 컨테이너출입 목록](<부두별 연간 컨테이너출입 목록/>) | 2025년 연간 컨테이너 출입 이력 | 동원(DGT), 한진(HJNC) |
| [동원 자부두 환적 데이터](<동원 자부두 환적 데이터/>) | 동원터미널 내 자부두 환적 자료 | DGT → DGT |
| [한진 자부두환적](<한진 자부두환적/>) | 한진터미널 내 자부두 환적 자료 | HJNC → HJNC |
| [동원·한진 타부두 환적 데이터](<동원·한진 타부두 환적 데이터/>) | 동원·한진 터미널 간 타부두 환적 자료 | DGT ↔ HJNC |
| [선석배정현황](<선석배정현황/>) | 2025년 부산 신항 7개 터미널 선석배정 자료 | BCT, BNCT, DGT, HJNC, HPNT, PNC, PNIT |

## 주요 파일

### 부두별 연간 컨테이너출입 목록

| 파일 | 내용 |
| :--- | :--- |
| `DGT_2025_anmual.txt` | 동원터미널의 2025년 연간 컨테이너 출입 목록 |
| `HJNC_2025_annual.txt` | 한진터미널의 2025년 연간 컨테이너 출입 목록 |

### 자부두·타부두 환적

| 구분 | 파일 |
| :--- | :--- |
| 동원 자부두 환적 | [DGT_DGT.csv](<동원 자부두 환적 데이터/DGT_DGT.csv>) |
| 한진 자부두 환적 | [hjnc_hjnc.csv](<한진 자부두환적/hjnc_hjnc.csv>) |
| 동원·한진 타부두 환적 | [DGT_HJNC.2025.csv](<동원·한진 타부두 환적 데이터/DGT_HJNC.2025.csv>) |

### 선석배정현황

| 터미널 | 파일 |
| :--- | :--- |
| BCT | `BCT_2025_선석배정현황.xlsx` |
| BNCT | `BNCT_2025_선석배정현황.xlsx` |
| DGT | `DGT_2025_선석배정현황.xlsx` |
| HJNC | `HJNC_2025_선석배정현황.xlsx` |
| HPNT | `HPNT_2025_선석배정현황.xls` |
| PNC | `PNC_선석배정현황_2025.xlsx` |
| PNIT | `PNIT_2025_선석배정현황.xls` |

## 데이터 다운로드

일부 대용량 파일은 **Git LFS**로 관리합니다. Git LFS를 설치한 환경에서 다음 순서로 내려받을 수 있습니다.

```bash
git lfs install
git clone https://github.com/busan-itt/data.git
cd data
git lfs pull
```

다운로드한 파일에 실제 데이터 대신 `version https://git-lfs.github.com/spec/v1` 문구가 보이면, 저장소 폴더에서 `git lfs pull`을 실행해 원본을 내려받아 주세요.

## 관련 저장소

[프로젝트 소개](https://github.com/busan-itt) · [데이터 분석](https://github.com/busan-itt/analysis) · [시뮬레이션](https://github.com/busan-itt/simulation)
