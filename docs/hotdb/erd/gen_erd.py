# -*- coding: utf-8 -*-
"""Hot DB 논리 모델(v9, Confluence 334201522) → DDL · DBML · ERD Editor(.erd.json) · Mermaid 생성.

원본: [공통] 내업 Hot DB 논리 모델 — 테이블 상세 (2026-10-08, 55번 v9)
실행: python gen_erd.py  (같은 폴더에 산출물 생성)
"""
import json
import time
import uuid
from pathlib import Path

OUT = Path(__file__).parent

# 열 형식: 이름|형|키|참조 대상|설명   (키: PK / FK / PK FK, 참조: 테이블.열)
SPEC = {
    # ── M 기준정보 ─────────────────────────────────────────
    "m_hull": ("M", "호선 1척", """
hull_no|text|PK||호선 번호(PROJ_NO)
ship_type|text|||선종
status|text|||건조 상태
in_scope|bool|||현재 기준점 포함 여부
"""),
    "m_block": ("M", "호선 × 블록 1개(대조 블록 기준). 위치는 BTS 지번과 센서 인식 두 축", """
hull_no|text|PK FK|m_hull.hull_no|호선
block_no|text|PK||블록 번호(3자리)
block_type|text|||평·곡·특수 등
weight_t|numeric|||블록 중량
current_trade_stage|text|||현재 공종·단계(가공/조립/의장 S·C·A·B/도장)
bts_lot|text|FK|m_lot.lot_code|BTS 기준 현재 지번(구역 수준)
bts_at|timestamptz|||BTS 마지막 운반 시각
sensed_location_id|text|FK|m_location.location_id|센서가 인식한 구역(ZONE)
sensed_x|numeric|||구역 안 인식 좌표 X
sensed_y|numeric|||구역 안 인식 좌표 Y
sensed_at|timestamptz|||마지막 인식 시각
"""),
    "m_location": ("M", "위치 1개 — 공장(SHOP) > 구역(ZONE) > 정반(JIG)", """
location_id|text|PK||SHOP: 공장코드 / ZONE: 공장‖구역 / JIG: 정반코드
level|text|||SHOP / ZONE / JIG
parent_id|text|FK|m_location.location_id|상위 위치(ZONE→SHOP, JIG→ZONE)
code|text|||공장코드·BAY/쉘터 번호·정반코드
name|text|||명칭
zone_type|text|||ZONE일 때 BAY / SHELTER
lot_code|text|FK|m_lot.lot_code|대표 지번(ZONE, 도장은 1:1)
geom|geometry|||좌표 폴리곤(ZONE만)
sensing_unit|bool|||장비·블록 위치 기준 구역 여부(ZONE)
attrs|jsonb|||레벨별 속성(SHOP: 사내외·공종 플래그 / JIG: 유형·작업센터·작업조·협력사)
valid_from|date|||유효 시작
valid_to|date|||유효 종료
"""),
    "m_lot": ("M", "BTS 지번 1개(야드 지번)", """
lot_code|text|PK||지번(= BTS LANDNO)
middle_lot|text|||중지번(공장·구역 단위)
lot_name|text|||명칭
use_type|text|||용도(조립공장·도장공장·의장공장·적치장 …)
process_type|text|||공정(조립·도장·의장·가공·PE)
indoor|bool|||옥내 여부
area_m2|numeric|||면적
geom|geometry|||폴리곤 좌표
location_id|text|FK|m_location.location_id|대응 구역(ZONE, 이름 기준 매핑)
"""),
    "m_device": ("M", "장비 개체 1대 + 현재 상태", """
device_id|text|PK||장비 ID(설비관리번호)
class_code|text|FK|m_code.code|유형 코드(DEVICE_CLASS)
category|text|||SENSOR / ACTUATOR / COMPUTE / UTILITY
parent_device_id|text|FK|m_device.device_id|상위 장비(팬틸트→라이다→엣지 PC)
location_id|text|FK|m_location.location_id|소속 구역(ZONE)
x|numeric|||좌표 X
y|numeric|||좌표 Y
z|numeric|||설치 높이
heading|numeric|||방향각
tilt|numeric|||기울기
attrs|jsonb|||유형별 속성(model·ip·fov_deg …)
last_seen|timestamptz|||마지막 신호 시각(집계)
online|bool|||온라인 여부(집계)
fault|bool|||고장 여부(집계)
fault_code|text|||현재 고장 코드
last_signals|jsonb|||최근 신호 값 {signal_code: {value, time}}
valid_from|date|||설치일
valid_to|date|||철거일
"""),
    "m_signal_def": ("M", "장비 유형 × 신호 1개 — 신호 사전·수집 경로·고장 규칙", """
class_code|text|PK FK|m_code.code|장비 유형('*' = 모든 장비 공통)
signal_code|text|PK||신호 코드
device_id|text|PK FK|m_device.device_id|특정 장비 예외일 때만(없으면 null)
name|text|||신호 이름
data_type|text|||num / bool / text / enum
unit|text|||단위
enum_values|text[]|||열거값
sample_period_s|int|||수집 주기(초)
adapter|text|||수집 경로(MQTT·Modbus·OPC-UA·REST·ISL TagId)
address_template|text|||주소 규칙({device_id}.{topic} 등)
normal_min|numeric|||정상 하한
normal_max|numeric|||정상 상한
fault_rule_type|text|||HEARTBEAT_TIMEOUT / FAULT_CODE / RANGE / ENUM / COMPOSITE
fault_params|jsonb|||규칙 인자
severity|text|||심각도
valid_from|date|||유효 시작
valid_to|date|||유효 종료
"""),
    "m_file_store": ("M", "파일 저장소 1개(NAS 마운트·기본 경로)", """
store_id|text|PK||저장소 ID
store_type|text|||RAW_PCD / ARTIFACT / CAD / IMAGE
base_uri|text|||기본 경로
path_rule|text|||하위 경로 규칙({shop}/{zone}/{yyyymmdd}/{scan_id})
retention_days|int|||보존 일수
owner|text|||관리 주체
"""),
    "m_cad_file": ("M", "경량 CAD 파일 1개", """
cad_file_id|text|PK||CAD 파일 ID
hull_no|text|FK|m_hull.hull_no|호선
block_no|text|FK|m_block.block_no|블록
cad_level|text|||PART·ASSY / OUTFIT_BLOCK·OUTFIT_MODULE·OUTFIT_ITEM
item_id|text|||대응 번호(조립 PART_NO)
store_id|text|FK|m_file_store.store_id|저장소
file_uri|text|||파일 경로(저장소 기준 상대)
fmt|text|||3DP / FBX / GLB / OBJ
rev_no|text|||리비전
received_at|timestamptz|||수신 시각
source_system|text|||제공 경로(3DP·DAVIEW·WSD)
valid|bool|||유효 여부
"""),
    "m_code": ("M", "코드 1개(코드 그룹 × 코드), 절점 정의 포함 · 레거시 복제", """
code_group|text|PK||TRADE / MP / STAGE / WSTG / ZONE / DEVICE_CLASS / CHECKPOINT
code|text|PK||코드 값(절점이면 공종‖단계‖순서)
name|text|||명칭
in_scope|bool|||내업 범위 여부
seq|int|||순서(절점·Stage)
parent_code|text|||상위 코드(절점의 공종·단계)
attrs|jsonb|||절점: judge_source·judge_rule·pct_at_cp / 송선: next_wstg
valid_from|date|||유효 시작
valid_to|date|||유효 종료
"""),
    # ── L 레거시 참조 ──────────────────────────────────────
    "l_daily_plan": ("L", "작업일 × 공종 × 앵커 1개 — 일일 작업계획", """
plan_date|date|PK||작업일
trade|text|PK||공종
anchor_type|text|PK||WSTG / GDO / PAINT_ORDER / DWG
anchor_id|text|PK||앵커 ID
hull_no|text|FK|m_hull.hull_no|호선
block_no|text|FK|m_block.block_no|블록
location_id|text|FK|m_location.location_id|작업 공장·구역·정반
org_code|text|||작업 조직
priority|int|||우선순위
wo_list|text[]|||연결된 W/O
source_system|text|||출처 화면·표
loaded_at|timestamptz|||적재 시각
"""),
    "l_work_unit": ("L", "작업 단위 1개 — 액티비티 > 오더·공정 > W/O(±1개월 기준점)", """
unit_id|text|PK||ACTIVITY: ZZACT / ORDER: AUFNR‖VORNR / WO: WORK_ORD_NO
level|text|||ACTIVITY / ORDER / WO
parent_id|text|FK|l_work_unit.unit_id|상위 단위(WO→ORDER→ACTIVITY)
act_no|text|||액티비티(W/O 앞 12자리 = GDO)
hull_no|text|FK|m_hull.hull_no|호선
block_no|text|FK|m_block.block_no|블록
trade|text|||공종
mp|text|||공정
stage|text|||Stage
wstg_code|text|||송선(조립)
location_id|text|FK|m_location.location_id|공장·정반
plan_mh|numeric|||계획시수(212) 또는 표준시수(411 STD_MH)
acc_rate|numeric|||누계 진도율(W/O)
plan_sd|date|||계획 착수
plan_fd|date|||계획 완료
actl_sd|date|||실적 착수
actl_fd|date|||실적 완료(마감 입력일)
attrs|jsonb|||W/O: 협력사·CARRYOVER / ORDER: 작업센터·DO 묶음
mnt_ts|timestamptz|||수정일시(워터마크)
"""),
    "l_wo_daily": ("L", "W/O × 실적일 × 작업 조직 1행 — 413 일자 시수 실적", """
wo_no|text|PK FK|l_work_unit.unit_id|W/O 번호(WO 레벨)
actl_date|date|PK||실적일
org_code|text|PK||작업 조직
daily_rate|numeric|||입력 진도율
daily_mh|numeric|||상응 시수
rgsr_ts|timestamptz|||등록일시(워터마크)
"""),
    "l_assy_node": ("L", "블록 조립 하이러키 노드 1개(블록·조립품·부재)", """
hull_no|text|PK FK|m_hull.hull_no|호선
block_no|text|PK FK|m_block.block_no|블록
node_id|text|PK||조립품 번호 또는 부재 번호
node_level|text|||BLOCK / 대조 / 중조 / 소조 / COMP / T-BAR / PART
node_kind|text|||ASSY / PART / STD (PART_NO_INDC)
parent_node_id|text|FK|l_assy_node.node_id|상위 노드
wstg_code|text|FK|m_code.code|이 노드가 만들어지는 송선(WSTG)
next_wstg|text|||다음 송선
shape_group|text|||동일 형상 그룹(카운팅 단위)
qty|int|||같은 형상 개수(목표)
weight_kg|numeric|||중량
cad_file_id|text|FK|m_cad_file.cad_file_id|대응 CAD
plan_sd|date|||월간계획 착수(210V)
"""),
    "l_cut_part": ("L", "가공 부재(절단 조각) 1개와 그 도면", """
part_no|text|PK||부재 번호(CHAR22)
hull_no|text|FK|m_hull.hull_no|호선
block_no|text|FK|m_block.block_no|블록
dwg_no|text|||절단 도면(가공 앵커)
cut_plan_date|date|||절단 계획일(310M)
cut_bay|text|||절단 BAY
cut_lot|text|||동일 형상 절단 LOT
dest_wstg|text|||배송지 송선
bend_flag|bool|||곡가공 대상(510M)
cut_done_ts|timestamptz|||절단 완료
group_ts|timestamptz|||모듬 등록
sort_ts|timestamptz|||부재 선별
issue_ts|timestamptz|||부재 불출(조립 투입)
inbound_ts|timestamptz|||사외 절단 부재 반입(301M)
cut_site|text|||사내 / 사외 절단
"""),
    "l_outfit_item": ("L", "의장 품목 1개(RSNUM‖RSPOS)", """
rsnum|text|PK||예약 번호
rspos|text|PK||예약 항목
act_no|text|FK|l_work_unit.act_no|GDO(ZZACT)
hull_no|text|FK|m_hull.hull_no|호선
block_no|text|FK|m_block.block_no|블록
stage|text|||Stage
wstg_code|text|||송선(S·C·A)
matnr|text|||품목 번호
class_code|text|||분류 코드(지지류/본체)
do_no|text|||DO 묶음(POTX1)
req_qty|numeric|||소요 수량
inst_qty|numeric|||완설 수량
inst_date|date|||설치 입력일(W033)
issue_ts|timestamptz|||불출 시각(031·072)
cad_file_id|text|FK|m_cad_file.cad_file_id|대응 CAD
aedat|date|||수정일(워터마크)
"""),
    "l_paint_element": ("L", "도장 W/O × 요소 1개(보조)", """
wo_no|text|PK FK|l_work_unit.unit_id|도장 W/O
elem_id|text|PK||요소 ID
elem_type|text|||블라스팅·스프레이 n차·FINAL
seq|int|||수순
done_date|date|||완료일
"""),
    "l_bts_move": ("L", "블록 운반 1건(반입·반출)", """
hull_no|text|PK FK|m_hull.hull_no|호선
block_no|text|PK FK|m_block.block_no|블록
move_ts|timestamptz|PK||운반 시각
from_lot|text|FK|m_lot.lot_code|출발 지번
to_lot|text|FK|m_lot.lot_code|도착 지번
move_type|text|||반입/반출
"""),
    # ── S 스캔·인식 ────────────────────────────────────────
    "s_scan_session": ("S", "스캔 회차 1회(같은 scan_id 메시지들의 헤더)", """
scan_id|text|PK||스캔 ID
location_id|text|FK|m_location.location_id|구역(ZONE)
scan_ts|timestamptz|||스캔 시각
device_ids|text[]|||참여 라이다
raw_store_id|text|FK|m_file_store.store_id|원시 PCD 저장소
raw_dir|text|||원시 PCD 디렉터리
artifact_store_id|text|FK|m_file_store.store_id|산출물 저장소
artifact_dir|text|||산출물 디렉터리
artifacts|jsonb|||[{type, uri, checksum}]
model_ver|text|||추론 모델 버전
first_msg_ts|timestamptz|||첫 메시지 수신
last_msg_ts|timestamptz|||마지막 메시지 수신
detection_cnt|int|||받은 인식 객체 수
expected_cnt|int|||예고된 객체 수
status|text|||OPEN / CLOSED / FAILED
"""),
    "s_scan_detection": ("S", "인식 객체 1개 = 실적 메시지 1건", """
scan_id|text|PK FK|s_scan_session.scan_id|스캔 회차
object_key|text|PK||객체 키(object_id, 없으면 수신 순번)
event_id|text|FK|ts.sensor_event.event_id|원문 메시지 ID, 1:1
received_ts|timestamptz|||수신 시각
object_type|text|||BLOCK / ASSY / PART / OUTFIT_ITEM / UNKNOWN
hull_no|text|FK|m_hull.hull_no|인식된 호선
block_no|text|FK|m_block.block_no|인식된 블록
item_id|text|FK|l_assy_node.node_id|조립품·부재·품목 ID(object_type에 따라 l_assy_node / l_cut_part / l_outfit_item)
anchor_type|text|||WSTG / GDO / DWG / PAINT_ORDER
anchor_id|text|FK|p_progress.anchor_id|매칭된 앵커
cad_file_id|text|FK|m_cad_file.cad_file_id|매칭에 쓴 CAD
match_conf|numeric|||CAD 매칭 신뢰도(0~1)
pose|jsonb|||{x,y,z,yaw} 또는 {matrix}
bbox|jsonb|||{l,w,h}
progress_est|numeric|||필드 추정 진행률
count_est|int|||인식 개수
presence|bool|||존재 여부(반출 판정)
stage_hint|text|||필드 스테이지
event_type|text|||START / PROGRESS / COMPLETE
segment_uri|text|||객체 세그먼트 PCD 경로
model_ver|text|||모델 버전
ocr_text|text|||OCR 판독 문자
judged|bool|||판별 반영 여부
judged_ts|timestamptz|||판별 시각
progress_ref|text|||반영된 p_progress 키(hull‖block‖anchor_type‖anchor_id)
"""),
    # ── T 시계열 ───────────────────────────────────────────
    "ts.device_signal": ("T", "장비 × 신호 × 시각 1점(하이퍼테이블)", """
time|timestamptz|PK||시각(파티션 키)
device_id|text|PK FK|m_device.device_id|장비(segmentby)
signal_code|text|PK FK|m_signal_def.signal_code|신호 코드(사건이면 규칙 코드)
kind|text|||SIGNAL / EVENT
value_num|numeric|||숫자 값
value_text|text|||문자 값(ONLINE·OFFLINE·FAULT_ON …)
quality|text|||품질
"""),
    "ts.sensor_event": ("T", "실적 메시지 원문 1건(하이퍼테이블)", """
time|timestamptz|PK||발생 시각(occurred_at)
event_id|text|PK||메시지 ID
scan_id|text|FK|s_scan_session.scan_id|스캔 회차
object_key|text|||객체 키
zone|text|||권역
stage|text|||스테이지
location_id|text|FK|m_location.location_id|구역
hull_no|text|FK|m_hull.hull_no|호선
block_no|text|FK|m_block.block_no|블록
object_type|text|||BLOCK / ASSY / PART / OUTFIT_ITEM
event_type|text|||START / PROGRESS / COMPLETE
source_system|text|||발생 시스템
idempotency_key|text|||scan_id‖object_key‖occurred_at
raw_payload|jsonb|||원문
"""),
    # ── P 실적 ─────────────────────────────────────────────
    "p_progress": ("P", "앵커 × 기준일 1행 · 레거시 복제", """
hull_no|text|PK FK|m_hull.hull_no|호선
block_no|text|PK FK|m_block.block_no|블록
anchor_type|text|PK||DWG / WSTG / GDO / PAINT_ORDER
anchor_id|text|PK||앵커 ID
as_of|date|PK||기준일
state|text|||notDue / inProgress / done / confirmed
auto_pct|numeric|||자동 산출 퍼센트
pct_basis|text|||COUNT / INSTALL / MH / DWG
legacy_pct|numeric|||레거시 진도율(비교값)
gap_pct|numeric|||자동 − 레거시
planned_qty|numeric|||목표
done_qty|numeric|||달성
last_cp|text|FK|m_code.code|마지막 도달 절점(CHECKPOINT)
cp_history|jsonb|||[{cp, reached_at, source, ref, conf, confirmed}]
evidence|jsonb|||근거 요약
location_id|text|FK|m_location.location_id|수행 공장·구역·정반
first_detect_ts|timestamptz|||첫 인식
last_detect_ts|timestamptz|||마지막 인식
confirmed_by|text|FK|p_action.action_id|확정 Key-In
"""),
    "p_progress_block": ("P", "블록 × 공종 × 단계 × 기준일 롤업 · 레거시 복제", """
hull_no|text|PK FK|m_hull.hull_no|호선
block_no|text|PK FK|m_block.block_no|블록
trade|text|PK||공종
stage|text|PK||단계
as_of|date|PK||기준일
auto_pct|numeric|||자동 실적률
legacy_pct|numeric|||레거시 실적률
weight_basis|text|||가중 기준
anchor_cnt|int|||앵커 수
open_action_cnt|int|||미해결 알림 수
bts_lot|text|FK|m_lot.lot_code|BTS 위치
sensed_location_id|text|FK|m_location.location_id|센서 인식 구역
"""),
    "p_item_state": ("P", "조립품·부재·의장 품목 1개의 현재 상태 · 레거시 복제", """
hull_no|text|PK FK|m_hull.hull_no|호선
block_no|text|PK FK|m_block.block_no|블록
item_type|text|PK||ASSY / PART / OUTFIT_ITEM
item_id|text|PK||node_id 또는 RSNUM+RSPOS
anchor_id|text|FK|p_progress.anchor_id|소속 앵커
state|text|||notDue / issued / detected / installed / carried
detect_ts|timestamptz|||센서 인식 시각
legacy_ts|timestamptz|||레거시 입력 시각
scan_id|text|FK|s_scan_session.scan_id|인식 회차
detection_seq|int|FK|s_scan_detection.object_key|인식 객체
"""),
    "p_action": ("P", "알림 1건 또는 Key-In 1건 · 레거시 복제", """
action_id|bigint|PK||ID
action_type|text|||EXCEPTION / KEYIN
hull_no|text|FK|m_hull.hull_no|호선
block_no|text|FK|m_block.block_no|블록
anchor_id|text|FK|p_progress.anchor_id|앵커
sub_type|text|||UNMATCHED·ORDER_REVERSED·GAP·NO_PLAN / PCT·CHECKPOINT·CORRECTION
detail|text|||내용
value_pct|numeric|||입력 퍼센트(Key-In)
cp_code|text|FK|m_code.code|확정 절점(Key-In)
reason_code|text|||사유
user_id|text|||입력·처리자
raised_at|timestamptz|||발생·입력 시각
status|text|||OPEN·RESOLVED / 입력·임시저장·확정
resolved_by|bigint|FK|p_action.action_id|해결한 Key-In ID
resolved_at|timestamptz|||해결 시각
"""),
}

# 예시 행: Confluence 테이블 상세의 "예시 행" 그대로. 첫 줄 = 열 이름, 마지막 줄 '#' = 출처 표기(★ 실측 / ☆ 가정)
EXAMPLES = {
    "m_hull": """
hull_no|ship_type|status|in_scope
5486|상선|건조 완료|false
# ★ 4호선 표본 실측
""",
    "m_block": """
hull_no|block_no|block_type|current_trade_stage|bts_lot|sensed_location_id|sensed_at
5486|731|평블록(PBS)|조립 G9 → 의장 A|(표본에 5486 운반 없음)|PBS‖5|☆ 2022-08-16 09:31
# ★ 블록·공장 실측, ☆ 센서 열은 예시
""",
    "m_location": """
location_id|level|parent_id|code|name|zone_type|geom|sensing_unit|attrs
PBS|SHOP||PBS|PBS(조립1)||||{in_out:C, use_assy:true}
PBS‖5|ZONE|PBS|5|5-BAY|BAY|☆ 제공 예정 폴리곤|true|
P502|JIG|PBS‖5|P502|5BAY 남,소조 취부/용접||||{work_cntr:P22A0_01, weg_code:22AE, vendor_flag:false}
# ★ 공장·BAY 명칭·정반 실측(ZPPT071, WC1210, YPWS120C). 정반 P502 → 2번째 글자 5 → 5-BAY
""",
    "m_lot": """
lot_code|middle_lot|lot_name|use_type|process_type|indoor|area_m2|location_id
3D1B01|3D1B|3DS 1-BAY 동쪽-01|조립공장|조립|true|3238|3DS‖1 (이름 기준 매핑)
# ★ 지번 마스터 실측. PBS 지번은 별도 확인
""",
    "m_device": """
device_id|class_code|category|parent_device_id|location_id|x|y|z|online|fault
LD-PBS-5-03|LIDAR|SENSOR|EPC-PBS-5-01|PBS‖5|☆ 123.4|☆ 56.7|☆ 8.0|true|false
# ☆ 장비는 전부 예시(설치 전)
""",
    "m_signal_def": """
class_code|signal_code|data_type|unit|enum_values|sample_period_s|adapter|address_template|normal_min|normal_max|fault_rule_type|fault_params|severity
*|heartbeat|bool|||10|||||HEARTBEAT_TIMEOUT|{timeout_s:60}|HIGH
LIDAR|scan_state|enum||[IDLE,SCANNING,ERROR]||MQTT|ot/sensor/{device_id}/state|||ENUM|{fault_values:[ERROR]}|
DEHUMID|humidity|num|%RH|||Modbus||0|60|RANGE||
# ☆ 예시
""",
    "m_file_store": """
store_id|store_type|base_uri|path_rule|retention_days
NAS_RAW|RAW_PCD|nfs://nas01/ot/raw|{shop}/{zone}/{yyyymmdd}/{scan_id}|31
# ☆ 예시
""",
    "m_cad_file": """
cad_file_id|hull_no|block_no|cad_level|item_id|store_id|file_uri|fmt|rev_no
CAD-5486-731-BK1A-A|5486|731|PART|5486-731-BK1A-A|NAS_CAD|5486/731/BK1A-A.glb|GLB|01
# ★ 부재 번호 실측, ☆ 파일 경로 예시
""",
    "m_code": """
code_group|code|name|in_scope|seq|parent_code|attrs
TRADE|S|조립|true|||
WSTG|S6|중조(S6)||||{next_wstg:G9}
CHECKPOINT|C‖J‖07|절단 완료(장비)||7|C‖J|{judge_source:LEGACY, judge_rule:'YPWC310M.CUT_MFG_FD 또는 YDEH060M.CUT_MFG_FD 입력'}
# ★ 코드 실측, 절점 코드는 초안
""",
    "l_daily_plan": """
plan_date|trade|anchor_type|anchor_id|location_id|priority|wo_list|source_system
2022-08-16|S|WSTG|5486‖731‖S6G9|P502|1|[5486S731A000S6FG9A, …WG9A, …GG9A]|☆ 조립 일일계획(미확인)
# ☆ 출처 미확인. 예시는 210V 월간계획 기준으로 구성
""",
    "l_work_unit": """
unit_id|level|parent_id|act_no|trade|mp|stage|wstg_code|location_id|plan_mh|acc_rate|plan_sd|plan_fd|actl_sd|actl_fd|attrs
5486S731A000|ACTIVITY||5486S731A000|S||B||PBS|425.7 h||2022-07-19|2022-08-26|||
000017254767‖0010|ORDER|5486S731A200|5486S731A200||||S6G9|P502|||2022-08-08|2022-08-10|||
5486S731A000S6FG9A|WO|000017254767‖0010|5486S731A000||||S6G9|P502|STD_MH(분)|100|||2022-08-11|2022-08-16|
000017351710‖0010|ORDER||5486W731GAG9 (GDO)|W|G|A||PBS|||2022-09-01|2022-09-07|2022-09-09|2022-09-13|
000017262345‖0010|ORDER||5486P731BP00|P|||||666||2022-09-19|2022-09-23|||{work_cntr:P62B0_01, oper_desc:'블록도장(도장)'}
# ★ 전부 실측(5486-731). 조립 W/O는 송선당 F 취부·W 용접·G 사상 3장
""",
    "l_wo_daily": """
wo_no|actl_date|org_code|daily_rate|daily_mh|rgsr_ts
5486W731GAG9001000|2022-09-09|ADA1|30|747|2022-09-14
5486W731GAG9001000|2022-09-12|ADA1|40|996|
5486W731GAG9001000|2022-09-13|ADA1|30|747|
# ★ 실측. 세 행 합 100%, 등록은 09-14 한 번(하루 단위 사후 입력의 전형)
""",
    "l_assy_node": """
node_id|node_level|node_kind|parent_node_id|wstg_code|next_wstg|qty|weight_kg|cad_file_id
5486-731|BLOCK|ASSY||G9||1|49,138|
5486-731-BK1A|소조|ASSY|5486-731-…(S6 조립품)|C1|S6|1|76.4|
5486-731-BK1A-A|PART|PART|5486-731-BK1A|C1||1||CAD-5486-731-BK1A-A
# ★ 실측: 040M 1단 48 → 2단 237 → 3단 69 노드, 조립품 78(A)·부재 274(P)·표준자재 2(Z)
""",
    "l_cut_part": """
part_no|dwg_no|cut_lot|dest_wstg|bend_flag|cut_done_ts|issue_ts|cut_site
5486-731-000-K1|(310M 연결 확인 필요)||G9|false|2022-08-15|2022-08-15|사내
5486-731-000-B1|||G9|true (F)|(미입력)||
# ★ 실측: 274 부재 중 8개만 절단일 입력(표본 기간 필터), 곡가공 대상 44
""",
    "l_outfit_item": """
rsnum|rspos|act_no|stage|wstg_code|matnr|class_code|do_no|req_qty|inst_qty|inst_date
0113910599|0069|5486W731GAG9|A|G9|5486TFTHR400A731|TFTHR(지지류)|5486W731AF2ZG9Z|1|1|2022-09-21
# ★ 실측: GDO 5486W731GAG9 품목 32, 설치 25(78%), DO 4묶음, 설치 입력일 09-21(W/O 완료 09-13보다 8일 뒤)
""",
    "l_bts_move": """
hull_no|block_no|move_ts|to_lot|move_type
4506|297|(요청일 미입력)|1DS202|반입
# ★ 표본 BTS는 다른 호선(4506 등)이라 5486-731 운반은 없음
""",
    "s_scan_session": """
scan_id|location_id|scan_ts|device_ids|raw_dir|detection_cnt|expected_cnt|status
S-20220816-PBS5-0931|PBS‖5|2022-08-16 09:31|[LD-PBS-5-03, LD-PBS-5-04]|PBS/5/20220816/S-…-0931|2|(종료 메시지 없음)|OPEN → CLOSED(유예 10분)
# ☆ 센서는 전부 예시
""",
    "s_scan_detection": """
scan_id|object_key|event_id|object_type|hull_no|block_no|item_id|anchor_type|anchor_id|cad_file_id|match_conf|pose|progress_est|count_est|judged|progress_ref
S-20220816-PBS5-0931|5486-731-BL34A|E-…-001|ASSY|5486|731|5486-731-BL34A|WSTG|5486‖731‖S6G9|CAD-5486-731-BL34A|0.91|{x:12.3, y:4.1, yaw:92}|0.80|1|true|5486‖731‖WSTG‖5486‖731‖S6G9
S-20220816-PBS5-0931|5486-731-BL34B|E-…-002|ASSY|||5486-731-BL34B||5486‖731‖S6G9||0.87||0.55|1|true|
# ☆ 예시(조립품 번호는 실측, 인식 값은 가정)
""",
    "ts.device_signal": """
time|device_id|signal_code|kind|value_text
2022-08-16 09:30:00|LD-PBS-5-03|scan_state|SIGNAL|SCANNING
2022-08-16 09:40:12|LD-PBS-5-03|heartbeat|EVENT|OFFLINE
# ☆ 예시
""",
    "ts.sensor_event": """
time|event_id|scan_id|object_key|zone|stage|location_id|hull_no|block_no|object_type|event_type|idempotency_key
2022-08-16 09:33:10|E-…-001|S-20220816-PBS5-0931|5486-731-BL34A|ASSEMBLY|FITTING|PBS‖5|5486|731|ASSY|PROGRESS|S-…-0931‖5486-731-BL34A‖20220816093310
# ☆ 예시
""",
    "p_progress": """
anchor_type|anchor_id|as_of|state|auto_pct|pct_basis|legacy_pct|gap_pct|planned_qty|done_qty|last_cp|cp_history|evidence|location_id
WSTG|5486‖731‖S6G9|2022-08-16|inProgress|☆ 64.3 (인식 9 ÷ 목표 14)|COUNT|100 (W/O ACCM_PRGS_RATE, 완료 08-16)|−35.7|14 (050M S6→G9 조립품)|☆ 9|||[{scan_id:S-…-0931, object_key:5486-731-BL34A}, …]|P502
GDO|5486W731GAG9|2022-09-21|inProgress|78.1 (설치 25 ÷ 소요 32, W033)|INSTALL|100 (W/O 413 누계 30+40+30)|−21.9|32|25||||PBS
PAINT_ORDER|5486‖731‖P|2022-09-30|done|(표본에 413 없음) = Σ413 ÷ (570+666)|MH|100 (W/O 완료 09-27·09-30)||1,236 MH|||||NO2
DWG|(310M 도면)|2022-08-15|done||DWG|||||C‖J‖10 부재 불출|[{cp:C‖J‖07 절단 완료, reached_at:2022-08-15, source:LEGACY, ref:YDEH060M.CUT_MFG_FD}, {cp:C‖J‖10, reached_at:2022-08-15, ref:CUT_ISS_FD}]||
# ★ 레거시 값 실측, ☆ 센서 값 예시. 조립 auto_pct는 인식 개수 가정
""",
    "p_progress_block": """
hull_no|block_no|trade|stage|as_of|auto_pct|legacy_pct|weight_basis|anchor_cnt|open_action_cnt|sensed_location_id
5486|731|S|B|2022-08-16|☆ 71.2 (송선 11개 STD_MH 가중)|100|W/O STD_MH|11|1|PBS‖5
5486|731|W|A|2022-09-21|78.1|100|W/O 시수|1||
# ★·☆ 혼합
""",
    "p_item_state": """
item_type|item_id|anchor_id|state|detect_ts|legacy_ts|scan_id|detection_seq
ASSY|5486-731-BL34A|5486‖731‖S6G9|detected|☆ 2022-08-16 09:33||S-…-0931|5486-731-BL34A
OUTFIT_ITEM|0113910599‖0069|5486W731GAG9|installed||2022-09-21||
# ★ 품목 실측, ☆ 인식 예시
""",
    "p_action": """
action_id|action_type|anchor_id|sub_type|detail|user_id|raised_at|status|resolved_by
1001|EXCEPTION|5486W731GAG9|GAP|W/O 누계 100%(09-13)인데 품목 설치 25/32 — 7개 미설치||2022-09-21|OPEN|
1002|KEYIN|5486W731GAG9|CORRECTION|미설치 7개는 A Stage 이월(carried) 처리|관리자||확정|
# ★ 수치 실측, 알림 문구는 예시
""",
}

# 적용 사례 5486호선 731 블록 — Confluence 메인 페이지 6절
SCENARIO = [
    ("기준점 적재", "하루 1회 전체 재조회. 731 블록이 ±1개월 기준점에 들어오면서 행이 만들어진다.", [
        ("m_block", "1행: 5486‖731, 현재 단계 '조립 B Stage'"),
        ("m_location", "PBS(SHOP) · PBS‖5(ZONE, 5-BAY) · P502·P704·P706·P301 …(JIG) — 이미 적재돼 있음"),
        ("l_work_unit", "조립: ACTIVITY 2행(5486S731A000 블록 조립 425.7 h 등) · ORDER 13행 · WO 34행(송선당 F·W·G 3장)"),
        ("l_work_unit", "의장: ORDER 000017351710‖0010 = GDO 5486W731GAG9(A Stage, 계획 09-01~09-07) · WO 5486W731GAG9001000"),
        ("l_work_unit", "도장: ORDER 전처리 570 MH · 도장 666 MH · WO 5486P731BC00C0001·BP00P0001(NO2)"),
        ("l_assy_node", "353행: 블록 1 → 1단 48 → 2단 237 → 3단 69. 송선별 목표 개수 S6→G9 14 …"),
        ("l_cut_part", "274행(060M). 절단 완료 8개(2022-08-15), 곡가공 대상 44"),
        ("l_outfit_item", "32행(RSNUM 0113910599). 소요 32, 설치 25(W033 09-21)"),
        ("l_daily_plan", "2022-08-16 조립 앵커 S6G9 우선순위 1 — 예시"),
    ]),
    ("센서 스캔", "2022-08-16 09:31 PBS 5-BAY. 라이다 2대가 조립품 2개를 인식해 메시지 2건이 온다(객체당 1건).", [
        ("ts.sensor_event", "2행(E-…-001, E-…-002). object_key 5486-731-BL34A / BL34B, raw_payload 원문"),
        ("s_scan_session", "1행 생성(첫 메시지). detection_cnt 1 → 2, OPEN → 마지막 메시지 10분 뒤 CLOSED"),
        ("s_scan_detection", "2행(메시지와 1:1). match_conf 0.91 / 0.87, anchor_id 5486‖731‖S6G9로 매칭"),
        ("ts.device_signal", "LD-PBS-5-03 scan_state SCANNING(09:30) → IDLE(09:34)"),
    ]),
    ("판별 (메시지마다)", "l_daily_plan에서 오늘 앵커 후보 → 조립품 번호가 l_assy_node에서 S6G9 송선 → 앵커 1행 갱신.", [
        ("p_progress", "WSTG 5486‖731‖S6G9 · 08-16: done_qty 7 → 9, auto_pct 50 → 64.3(÷ 목표 14), legacy_pct 100, gap −35.7"),
        ("p_item_state", "BL34A·BL34B 2행 state detected"),
        ("s_scan_detection", "judged true, progress_ref 기록"),
        ("p_action", "없음(정상)"),
    ]),
    ("레거시 판별 (하루 1회)", "의장·도장·가공은 레거시 적재 뒤 하루 1회 판별.", [
        ("p_progress", "GDO 5486W731GAG9 · 09-21: auto_pct 78.1(설치 25 ÷ 32), legacy_pct 100(413 누계 30+40+30)"),
        ("p_action", "GAP 알림 1건: W/O는 100%인데 품목 7개 미설치 → 관리자 Key-In 'A Stage 이월' 확정, p_item_state 7행 carried"),
        ("p_progress", "PAINT_ORDER 5486‖731‖P: 분모 570+666 = 1,236 MH. 표본에 도장 413이 없어 분자 미산출"),
        ("p_progress", "DWG: cp_history에 절단 완료(08-15)·부재 불출(08-15) 적재"),
    ]),
    ("롤업 · 복제", "회차가 닫히거나 하루 1회.", [
        ("p_progress_block", "5486‖731‖S‖B · 08-16: 송선 11개를 STD_MH로 가중 → 71.2 ☆, legacy 100, open_action_cnt 1"),
        ("p_progress_block", "5486‖731‖W‖A · 09-21: 78.1 / 100"),
        ("p_progress", "p_progress·p_progress_block·p_item_state·p_action + m_code(CHECKPOINT)를 레거시 DB에 같은 구조로 UPSERT"),
    ]),
]


def parse_examples():
    ex = {}
    for name, body in EXAMPLES.items():
        lines = body.strip().splitlines()
        note = lines[-1][1:].strip() if lines[-1].startswith("#") else ""
        rows = [l.split("|") for l in lines if not l.startswith("#")]
        head, data = rows[0], rows[1:]
        for r in data:
            assert len(r) == len(head), (name, r)
        ex[name] = {"head": head, "rows": data, "note": note}
    return ex


EX = parse_examples()


def ex_values(table, col):
    """그 열의 예시 값들(빈 값 제외, 순서 유지, 중복 제거)."""
    e = EX.get(table)
    if not e or col not in e["head"]:
        return []
    i = e["head"].index(col)
    seen = []
    for r in e["rows"]:
        v = r[i].strip()
        if v and v not in seen:
            seen.append(v)
    return seen


def with_example(table, c):
    vals = ex_values(table, c["name"])
    return c["comment"] + (f" (예: {' / '.join(vals[:3])})" if vals else "")


AREAS = {
    "M": "기준정보", "L": "레거시 참조", "S": "스캔·인식", "T": "시계열", "P": "실적",
}


def parse():
    tables = {}
    for name, (area, desc, body) in SPEC.items():
        cols = []
        for line in body.strip().splitlines():
            n, t, k, ref, cmt = line.split("|")
            cols.append({"name": n, "type": t, "pk": "PK" in k, "fk": "FK" in k,
                         "ref": ref or None, "comment": cmt})
        tables[name] = {"name": name, "area": area, "desc": desc, "cols": cols}
    return tables


def pk_of(t):
    return [c["name"] for c in t["cols"] if c["pk"]]


def relations(tables):
    """논리 관계 목록. hull_no+block_no 쌍은 m_block 하나로 묶는다.

    enforced=True 는 참조 대상이 PK 전체와 맞아 실제 FK 제약을 걸 수 있는 관계.
    """
    rels = []
    for t in tables.values():
        cols = {c["name"]: c for c in t["cols"]}
        has_block = "block_no" in cols and cols["block_no"]["ref"] == "m_block.block_no"
        grouped = {}
        for c in t["cols"]:
            if not c["ref"]:
                continue
            tgt, tcol = c["ref"].rsplit(".", 1)
            if has_block and c["name"] == "hull_no" and tgt == "m_hull":
                continue  # m_block 관계에 흡수
            src_cols, tgt_cols = [c["name"]], [tcol]
            if tgt == "m_block":
                src_cols, tgt_cols = ["hull_no", "block_no"], ["hull_no", "block_no"]
            if tgt == "l_assy_node" and t["name"] == "l_assy_node":
                src_cols, tgt_cols = ["hull_no", "block_no", "parent_node_id"], ["hull_no", "block_no", "node_id"]
            key = (tgt, tuple(src_cols))
            grouped[key] = (tgt, src_cols, tgt_cols)
        for tgt, src_cols, tgt_cols in grouped.values():
            tt = tables[tgt]
            tcols = {c["name"]: c for c in tt["cols"]}
            types_ok = all(tcols[tc]["type"] == cols[sc]["type"] for sc, tc in zip(src_cols, tgt_cols))
            enforced = sorted(tgt_cols) == sorted(pk_of(tt)) and types_ok
            identifying = all(cols[sc]["pk"] for sc in src_cols)
            rels.append({"src": t["name"], "src_cols": src_cols, "tgt": tgt,
                         "tgt_cols": tgt_cols, "enforced": enforced, "identifying": identifying})
    return rels


def qname(n):
    return n if n.startswith("ts.") else f"hot.{n}"


def sql_literal(s):
    return "'" + s.replace("'", "''") + "'"


def gen_sql(tables, rels):
    out = ["-- Hot DB 논리 모델 v9 (Confluence 334201522, 2026-10-08) 에서 생성한 DDL",
           "-- 논리 모델이라 실제 FK 는 참조 대상이 PK 와 맞는 관계만 건다. 나머지는 COMMENT 로 남긴다.",
           "-- 생성: docs/hotdb/erd/gen_erd.py", "",
           "CREATE EXTENSION IF NOT EXISTS postgis;", "CREATE SCHEMA IF NOT EXISTS hot;",
           "CREATE SCHEMA IF NOT EXISTS ts;", ""]
    for t in tables.values():
        pk = pk_of(t)
        nullable_pk = t["name"] == "m_signal_def"  # device_id 는 예외 정의일 때만 값이 있다
        lines = []
        for c in t["cols"]:
            nn = " NOT NULL" if c["pk"] and not (nullable_pk and c["name"] == "device_id") else ""
            lines.append(f"    {c['name']} {c['type']}{nn}")
        if nullable_pk:
            lines.append(f"    CONSTRAINT {t['name']}_uk UNIQUE NULLS NOT DISTINCT ({', '.join(pk)})")
        else:
            lines.append(f"    PRIMARY KEY ({', '.join(pk)})")
        out.append(f"CREATE TABLE {qname(t['name'])} (\n" + ",\n".join(lines) + "\n);")
        out.append(f"COMMENT ON TABLE {qname(t['name'])} IS {sql_literal('[' + t['area'] + '] ' + t['desc'])};")
        for c in t["cols"]:
            cmt = with_example(t["name"], c) + (f" (→ {c['ref']})" if c["ref"] else "")
            out.append(f"COMMENT ON COLUMN {qname(t['name'])}.{c['name']} IS {sql_literal(cmt)};")
        out.append("")
    out.append("-- 하이퍼테이블 (TimescaleDB)")
    out.append("-- SELECT create_hypertable('ts.device_signal', 'time', chunk_time_interval => interval '6 hours');")
    out.append("-- SELECT create_hypertable('ts.sensor_event', 'time');")
    out.append("")
    out.append("-- FK 제약 (참조 대상 PK 와 일치하는 관계만)")
    for i, r in enumerate(x for x in rels if x["enforced"]):
        out.append(f"ALTER TABLE {qname(r['src'])} ADD CONSTRAINT fk_{r['src'].replace('.', '_')}_{i} "
                   f"FOREIGN KEY ({', '.join(r['src_cols'])}) REFERENCES {qname(r['tgt'])} ({', '.join(r['tgt_cols'])});")
    out.append("")
    out.append("-- 논리 참조만 (PK 불일치·형 불일치·다형 참조라 제약 없음)")
    for r in rels:
        if not r["enforced"]:
            out.append(f"--   {r['src']}({', '.join(r['src_cols'])}) → {r['tgt']}({', '.join(r['tgt_cols'])})")
    (OUT / "hotdb.sql").write_text("\n".join(out) + "\n", encoding="utf-8")


def gen_dbml(tables, rels):
    out = ['Project hotdb {\n  database_type: "PostgreSQL"\n  Note: "내업 Hot DB 논리 모델 v9 (Confluence 334201522)"\n}', ""]
    for area, label in AREAS.items():
        names = [f'"{n}"' if "." in n else n for n, t in tables.items() if t["area"] == area]
        out.append(f"TableGroup {area}_{label.replace('·', '_').replace(' ', '_')} {{\n  " + "\n  ".join(names) + "\n}\n")
    for t in tables.values():
        tn = f'"{t["name"]}"' if "." in t["name"] else t["name"]
        out.append(f"Table {tn} [note: '{t['desc']}'] {{")
        for c in t["cols"]:
            typ = f'"{c["type"]}"' if "[" in c["type"] else c["type"]
            note = with_example(t["name"], c).replace("'", "\\'")
            out.append(f"  {c['name']} {typ} [note: '{note}']")
        out.append(f"  indexes {{\n    ({', '.join(pk_of(t))}) [pk]\n  }}\n}}\n")
    for r in rels:
        s = f'"{r["src"]}"' if "." in r["src"] else r["src"]
        g = f'"{r["tgt"]}"' if "." in r["tgt"] else r["tgt"]
        if len(r["src_cols"]) == 1:
            out.append(f"Ref: {s}.{r['src_cols'][0]} > {g}.{r['tgt_cols'][0]}")
        else:
            out.append(f"Ref: {s}.({', '.join(r['src_cols'])}) > {g}.({', '.join(r['tgt_cols'])})")
    (OUT / "hotdb.dbml").write_text("\n".join(out) + "\n", encoding="utf-8")


def layout(tables):
    """영역별로 열을 나눠 쌓는다. 표 높이 ≈ 머리 + 열 수 × 24px."""
    pos, x = {}, 60
    col_w, max_h = 720, 1500
    for area in AREAS:
        y, area_x = 60, x
        for t in (t for t in tables.values() if t["area"] == area):
            h = 70 + 24 * len(t["cols"])
            if y + h > max_h and y > 60:
                area_x += col_w
                y = 60
            pos[t["name"]] = (area_x, y)
            y += h + 60
        x = area_x + col_w + 120
    return pos


def gen_erd_editor(tables, rels):
    """ERD Editor(dineug.vuerd-vscode) v3 형식."""
    now = int(time.time() * 1000)
    meta = {"updateAt": now, "createAt": now}
    pos = layout(tables)
    nid = lambda: uuid.uuid4().hex[:21]
    tbl_ent, col_ent, rel_ent = {}, {}, {}
    tid, cid = {}, {}
    colors = {"M": "#F28C28", "L": "#4A90D9", "S": "#2BAE66", "T": "#8E9AAF", "P": "#E04F5F"}
    for z, t in enumerate(tables.values(), 1):
        tid[t["name"]] = nid()
        ids = []
        for c in t["cols"]:
            i = nid()
            cid[(t["name"], c["name"])] = i
            ids.append(i)
            opts = (2 | 8) if c["pk"] else 0  # primaryKey | notNull
            keys = (1 if c["pk"] else 0) | (2 if c["fk"] else 0)
            col_ent[i] = {"id": i, "tableId": tid[t["name"]], "name": c["name"], "comment": with_example(t["name"], c),
                          "dataType": c["type"], "default": "", "options": opts,
                          "ui": {"keys": keys, "widthName": 120, "widthComment": 260,
                                 "widthDataType": 90, "widthDefault": 60}, "meta": meta}
        x, y = pos[t["name"]]
        tbl_ent[tid[t["name"]]] = {"id": tid[t["name"]], "name": t["name"],
                                   "comment": f"[{t['area']}] {t['desc']}",
                                   "columnIds": ids, "seqColumnIds": list(ids),
                                   "ui": {"x": x, "y": y, "zIndex": z, "widthName": 140,
                                          "widthComment": 300, "color": colors[t["area"]]},
                                   "meta": meta}
    for r in rels:
        i = nid()
        sx, sy = pos[r["tgt"]]
        ex, ey = pos[r["src"]]
        rel_ent[i] = {"id": i, "identification": r["identifying"],
                      "relationshipType": 2,  # ZeroN
                      "startRelationshipType": 2,  # dash
                      "start": {"tableId": tid[r["tgt"]],
                                "columnIds": [cid[(r["tgt"], c)] for c in r["tgt_cols"]],
                                "x": sx, "y": sy, "direction": 2},
                      "end": {"tableId": tid[r["src"]],
                              "columnIds": [cid[(r["src"], c)] for c in r["src_cols"]],
                              "x": ex, "y": ey, "direction": 1},
                      "meta": meta}
    doc = {
        "$schema": "https://raw.githubusercontent.com/dineug/erd-editor/main/json-schema/schema.json",
        "version": "3.0.0",
        "settings": {"width": 6000, "height": 2400, "scrollTop": 0, "scrollLeft": 0, "zoomLevel": 0.6,
                     "show": 431, "database": 16, "databaseName": "hotdb", "canvasType": "ERD",
                     "language": 1, "tableNameCase": 4, "columnNameCase": 2, "bracketType": 1,
                     "relationshipDataTypeSync": True, "relationshipOptimization": False,
                     "columnOrder": [1, 2, 4, 8, 16, 32, 64], "maxWidthComment": -1,
                     "ignoreSaveSettings": 0},
        "doc": {"tableIds": list(tbl_ent), "relationshipIds": list(rel_ent),
                "indexIds": [], "memoIds": []},
        "collections": {"tableEntities": tbl_ent, "tableColumnEntities": col_ent,
                        "relationshipEntities": rel_ent, "indexEntities": {},
                        "indexColumnEntities": {}, "memoEntities": {}},
        "lww": {},
    }
    (OUT / "hotdb.erd.json").write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")


def mm_id(n):
    return n.replace(".", "_")


def mermaid(tables, rels, area=None):
    """area 가 None 이면 전체 관계도(열 없이), 아니면 해당 영역 표의 전체 열 + 바깥 표는 이름만."""
    lines = ["erDiagram"]
    inner = [n for n, t in tables.items() if area is None or t["area"] == area]
    rs = [r for r in rels if area is None or r["src"] in inner]
    outer = sorted({r["tgt"] for r in rs} - set(inner))
    for n in inner + outer:
        t = tables[n]
        label = f'{mm_id(n)}["{n}"]'
        if area is None or n in outer:
            lines.append(f"    {label}")
            continue
        lines.append(f"    {label} {{")
        for c in t["cols"]:
            keys = ",".join(k for k, v in (("PK", c["pk"]), ("FK", c["fk"])) if v)
            typ = c["type"].replace("[]", "_arr")
            lines.append(f'        {typ} {c["name"]} {keys}'.rstrip())
        lines.append("    }")
    for r in rs:
        conn = "||--o{" if r["identifying"] else "|o--o{"
        if not r["enforced"]:
            conn = conn.replace("--", "..")
        lines.append(f'    {mm_id(r["tgt"])} {conn} {mm_id(r["src"])} : "{",".join(r["src_cols"])}"')
    return "\n".join(lines)


def gen_mermaid(tables, rels):
    md = ["# Hot DB 논리 모델 v9 ERD", "",
          "원본: Confluence `[공통] 내업 Hot DB 논리 모델 — 테이블 상세` (334201522). 점선 = 제약 없는 논리 참조.", "",
          "## 전체 관계", "", "```mermaid", mermaid(tables, rels), "```", ""]
    for a, label in AREAS.items():
        md += [f"## {a} {label}", "", "```mermaid", mermaid(tables, rels, a), "```", ""]
    (OUT / "hotdb-erd.md").write_text("\n".join(md), encoding="utf-8")
    data = {"areas": AREAS, "tables": list(tables.values()), "rels": rels,
            "mermaid": {"ALL": mermaid(tables, rels), **{a: mermaid(tables, rels, a) for a in AREAS}}}
    (OUT / "hotdb-erd.data.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def gen_html(tables, rels):
    from html import escape as e
    tpl = (OUT / "hotdb-erd.template.html").read_text(encoding="utf-8")
    nav, sections = [], []
    link = lambda n: f'<a href="#t-{mm_id(n)}"><code>{e(n)}</code></a>'
    nav.append('<a class="chip" href="#story">예시 따라가기</a>')
    steps = []
    for i, (title, lead, items) in enumerate(SCENARIO, 1):
        lis = "".join(f'<li>{link(n)}<span>{e(txt)}</span></li>' for n, txt in items)
        steps.append(f'<li class="step"><div class="step-no">{i}</div><div class="step-body">'
                     f'<h3>{e(title)}</h3><p>{e(lead)}</p><ul>{lis}</ul></div></li>')
    sections.append(
        '<section id="story" class="area"><header class="area-head"><span class="tag">예시</span>'
        '<h2>5486호선 731 블록이 표를 지나가는 순서</h2>'
        '<p>평블록, PBS(조립1). 조립 송선 11개 · 조립품 78 · 가공 부재 274 · 의장 GDO 1(품목 32) · 도장 오더 2. '
        '2022-07~09 작업. 레거시 값은 4호선 표본 실측, ☆ 센서 값은 장비 설치 전이라 가정값.</p></header>'
        f'<ol class="steps">{"".join(steps)}</ol></section>')
    story = sections.pop()
    nav.append('<a class="chip" href="#erd">관계도</a>')
    for a, label in AREAS.items():
        ts_ = [t for t in tables.values() if t["area"] == a]
        nav.append(f'<a class="chip" href="#area-{a}" data-area="{a}"><b>{a}</b> {label} <span>{len(ts_)}</span></a>')
        cards = []
        for t in ts_:
            rows = []
            for c in t["cols"]:
                key = "".join(f'<span class="k k-{k.lower()}">{k}</span>' for k, v in (("PK", c["pk"]), ("FK", c["fk"])) if v)
                ref = f'<code>{e(c["ref"])}</code>' if c["ref"] else ""
                vals = ex_values(t["name"], c["name"])
                exv = "".join(f'<span class="ev">{e(v)}</span>' for v in vals[:3])
                rows.append(f'<tr><td class="cn"><code>{e(c["name"])}</code></td><td class="ty">{e(c["type"])}</td>'
                            f'<td class="ky">{key}</td><td class="cm">{e(c["comment"])}'
                            f'{f"<div class=rf>→ {ref}</div>" if ref else ""}</td><td class="ex">{exv}</td></tr>')
            exm = EX.get(t["name"])
            if exm:
                head = "".join(f"<th><code>{e(h)}</code></th>" for h in exm["head"])
                body = "".join("<tr>" + "".join(f"<td>{e(v)}</td>" for v in r) + "</tr>" for r in exm["rows"])
                ex_html = (f'<details class="rows" open><summary>예시 행 {len(exm["rows"])}개 · {e(exm["note"])}</summary>'
                           f'<div class="scroll"><table class="extable"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></details>')
            else:
                ex_html = '<p class="noex">원문에 예시 행 없음(보조 표).</p>'
            cards.append(
                f'<article class="tbl" id="t-{mm_id(t["name"])}" data-search="{e(t["name"] + " " + t["desc"] + " " + " ".join(c["name"] + " " + c["comment"] for c in t["cols"]))}">'
                f'<h3><code>{e(t["name"])}</code><span class="pk">PK ({e(", ".join(pk_of(t)))})</span>'
                f'<button type="button" class="show" data-show="{e(t["name"])}">관계도에서 보기</button></h3>'
                f'<p class="desc">{e(t["desc"])}</p>'
                f'<div class="scroll"><table class="cols"><thead><tr><th>열</th><th>형</th><th>키</th><th>설명 · 참조 대상</th><th>예시 값</th></tr></thead>'
                f'<tbody>{"".join(rows)}</tbody></table></div>{ex_html}</article>')
        sections.append(
            f'<section id="area-{a}" class="area" data-area="{a}"><header class="area-head"><span class="tag">{a}</span>'
            f'<h2>{label}</h2><p>{len(ts_)}개 표. 열 전체, 참조 대상, 예시 값.</p></header>'
            f'<div class="cards">{"".join(cards)}</div></section>')
    stats = {"tables": len(tables), "cols": sum(len(t["cols"]) for t in tables.values()),
             "rels": len(rels), "fks": sum(r["enforced"] for r in rels)}
    data = {"areas": AREAS, "rels": rels,
            "tables": [{**t, "example": EX.get(t["name"])} for t in tables.values()]}
    data_js = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    html = (tpl.replace("{{NAV}}", "\n".join(nav)).replace("{{SECTIONS}}", "\n".join(sections))
            .replace("{{STORY}}", story).replace("{{DATA}}", data_js)
            .replace("{{TABLES}}", str(stats["tables"])).replace("{{COLS}}", str(stats["cols"]))
            .replace("{{RELS}}", str(stats["rels"])).replace("{{FKS}}", str(stats["fks"])))
    (OUT / "hotdb-erd.html").write_text(html, encoding="utf-8")


if __name__ == "__main__":
    T = parse()
    R = relations(T)
    gen_sql(T, R)
    gen_dbml(T, R)
    gen_erd_editor(T, R)
    gen_mermaid(T, R)
    gen_html(T, R)
    print(f"tables={len(T)} cols={sum(len(t['cols']) for t in T.values())} "
          f"rels={len(R)} enforced={sum(r['enforced'] for r in R)}")
