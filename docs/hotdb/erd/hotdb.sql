-- Hot DB 논리 모델 v9 (Confluence 334201522, 2026-10-08) 에서 생성한 DDL
-- 논리 모델이라 실제 FK 는 참조 대상이 PK 와 맞는 관계만 건다. 나머지는 COMMENT 로 남긴다.
-- 생성: docs/hotdb/erd/gen_erd.py

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS hot;
CREATE SCHEMA IF NOT EXISTS ts;

CREATE TABLE hot.m_hull (
    hull_no text NOT NULL,
    ship_type text,
    status text,
    in_scope bool,
    PRIMARY KEY (hull_no)
);
COMMENT ON TABLE hot.m_hull IS '[M] 호선 1척';
COMMENT ON COLUMN hot.m_hull.hull_no IS '호선 번호(PROJ_NO) (예: 5486)';
COMMENT ON COLUMN hot.m_hull.ship_type IS '선종 (예: 상선)';
COMMENT ON COLUMN hot.m_hull.status IS '건조 상태 (예: 건조 완료)';
COMMENT ON COLUMN hot.m_hull.in_scope IS '현재 기준점 포함 여부 (예: false)';

CREATE TABLE hot.m_block (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    block_type text,
    weight_t numeric,
    current_trade_stage text,
    bts_lot text,
    bts_at timestamptz,
    sensed_location_id text,
    sensed_x numeric,
    sensed_y numeric,
    sensed_at timestamptz,
    PRIMARY KEY (hull_no, block_no)
);
COMMENT ON TABLE hot.m_block IS '[M] 호선 × 블록 1개(대조 블록 기준). 위치는 BTS 지번과 센서 인식 두 축';
COMMENT ON COLUMN hot.m_block.hull_no IS '호선 (예: 5486) (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.m_block.block_no IS '블록 번호(3자리) (예: 731)';
COMMENT ON COLUMN hot.m_block.block_type IS '평·곡·특수 등 (예: 평블록(PBS))';
COMMENT ON COLUMN hot.m_block.weight_t IS '블록 중량';
COMMENT ON COLUMN hot.m_block.current_trade_stage IS '현재 공종·단계(가공/조립/의장 S·C·A·B/도장) (예: 조립 G9 → 의장 A)';
COMMENT ON COLUMN hot.m_block.bts_lot IS 'BTS 기준 현재 지번(구역 수준) (예: (표본에 5486 운반 없음)) (→ m_lot.lot_code)';
COMMENT ON COLUMN hot.m_block.bts_at IS 'BTS 마지막 운반 시각';
COMMENT ON COLUMN hot.m_block.sensed_location_id IS '센서가 인식한 구역(ZONE) (예: PBS‖5) (→ m_location.location_id)';
COMMENT ON COLUMN hot.m_block.sensed_x IS '구역 안 인식 좌표 X';
COMMENT ON COLUMN hot.m_block.sensed_y IS '구역 안 인식 좌표 Y';
COMMENT ON COLUMN hot.m_block.sensed_at IS '마지막 인식 시각 (예: ☆ 2022-08-16 09:31)';

CREATE TABLE hot.m_location (
    location_id text NOT NULL,
    level text,
    parent_id text,
    code text,
    name text,
    zone_type text,
    lot_code text,
    geom geometry,
    sensing_unit bool,
    attrs jsonb,
    valid_from date,
    valid_to date,
    PRIMARY KEY (location_id)
);
COMMENT ON TABLE hot.m_location IS '[M] 위치 1개 — 공장(SHOP) > 구역(ZONE) > 정반(JIG)';
COMMENT ON COLUMN hot.m_location.location_id IS 'SHOP: 공장코드 / ZONE: 공장‖구역 / JIG: 정반코드 (예: PBS / PBS‖5 / P502)';
COMMENT ON COLUMN hot.m_location.level IS 'SHOP / ZONE / JIG (예: SHOP / ZONE / JIG)';
COMMENT ON COLUMN hot.m_location.parent_id IS '상위 위치(ZONE→SHOP, JIG→ZONE) (예: PBS / PBS‖5) (→ m_location.location_id)';
COMMENT ON COLUMN hot.m_location.code IS '공장코드·BAY/쉘터 번호·정반코드 (예: PBS / 5 / P502)';
COMMENT ON COLUMN hot.m_location.name IS '명칭 (예: PBS(조립1) / 5-BAY / 5BAY 남,소조 취부/용접)';
COMMENT ON COLUMN hot.m_location.zone_type IS 'ZONE일 때 BAY / SHELTER (예: BAY)';
COMMENT ON COLUMN hot.m_location.lot_code IS '대표 지번(ZONE, 도장은 1:1) (→ m_lot.lot_code)';
COMMENT ON COLUMN hot.m_location.geom IS '좌표 폴리곤(ZONE만) (예: ☆ 제공 예정 폴리곤)';
COMMENT ON COLUMN hot.m_location.sensing_unit IS '장비·블록 위치 기준 구역 여부(ZONE) (예: true)';
COMMENT ON COLUMN hot.m_location.attrs IS '레벨별 속성(SHOP: 사내외·공종 플래그 / JIG: 유형·작업센터·작업조·협력사) (예: {in_out:C, use_assy:true} / {work_cntr:P22A0_01, weg_code:22AE, vendor_flag:false})';
COMMENT ON COLUMN hot.m_location.valid_from IS '유효 시작';
COMMENT ON COLUMN hot.m_location.valid_to IS '유효 종료';

CREATE TABLE hot.m_lot (
    lot_code text NOT NULL,
    middle_lot text,
    lot_name text,
    use_type text,
    process_type text,
    indoor bool,
    area_m2 numeric,
    geom geometry,
    location_id text,
    PRIMARY KEY (lot_code)
);
COMMENT ON TABLE hot.m_lot IS '[M] BTS 지번 1개(야드 지번)';
COMMENT ON COLUMN hot.m_lot.lot_code IS '지번(= BTS LANDNO) (예: 3D1B01)';
COMMENT ON COLUMN hot.m_lot.middle_lot IS '중지번(공장·구역 단위) (예: 3D1B)';
COMMENT ON COLUMN hot.m_lot.lot_name IS '명칭 (예: 3DS 1-BAY 동쪽-01)';
COMMENT ON COLUMN hot.m_lot.use_type IS '용도(조립공장·도장공장·의장공장·적치장 …) (예: 조립공장)';
COMMENT ON COLUMN hot.m_lot.process_type IS '공정(조립·도장·의장·가공·PE) (예: 조립)';
COMMENT ON COLUMN hot.m_lot.indoor IS '옥내 여부 (예: true)';
COMMENT ON COLUMN hot.m_lot.area_m2 IS '면적 (예: 3238)';
COMMENT ON COLUMN hot.m_lot.geom IS '폴리곤 좌표';
COMMENT ON COLUMN hot.m_lot.location_id IS '대응 구역(ZONE, 이름 기준 매핑) (예: 3DS‖1 (이름 기준 매핑)) (→ m_location.location_id)';

CREATE TABLE hot.m_device (
    device_id text NOT NULL,
    class_code text,
    category text,
    parent_device_id text,
    location_id text,
    x numeric,
    y numeric,
    z numeric,
    heading numeric,
    tilt numeric,
    attrs jsonb,
    last_seen timestamptz,
    online bool,
    fault bool,
    fault_code text,
    last_signals jsonb,
    valid_from date,
    valid_to date,
    PRIMARY KEY (device_id)
);
COMMENT ON TABLE hot.m_device IS '[M] 장비 개체 1대 + 현재 상태';
COMMENT ON COLUMN hot.m_device.device_id IS '장비 ID(설비관리번호) (예: LD-PBS-5-03)';
COMMENT ON COLUMN hot.m_device.class_code IS '유형 코드(DEVICE_CLASS) (예: LIDAR) (→ m_code.code)';
COMMENT ON COLUMN hot.m_device.category IS 'SENSOR / ACTUATOR / COMPUTE / UTILITY (예: SENSOR)';
COMMENT ON COLUMN hot.m_device.parent_device_id IS '상위 장비(팬틸트→라이다→엣지 PC) (예: EPC-PBS-5-01) (→ m_device.device_id)';
COMMENT ON COLUMN hot.m_device.location_id IS '소속 구역(ZONE) (예: PBS‖5) (→ m_location.location_id)';
COMMENT ON COLUMN hot.m_device.x IS '좌표 X (예: ☆ 123.4)';
COMMENT ON COLUMN hot.m_device.y IS '좌표 Y (예: ☆ 56.7)';
COMMENT ON COLUMN hot.m_device.z IS '설치 높이 (예: ☆ 8.0)';
COMMENT ON COLUMN hot.m_device.heading IS '방향각';
COMMENT ON COLUMN hot.m_device.tilt IS '기울기';
COMMENT ON COLUMN hot.m_device.attrs IS '유형별 속성(model·ip·fov_deg …)';
COMMENT ON COLUMN hot.m_device.last_seen IS '마지막 신호 시각(집계)';
COMMENT ON COLUMN hot.m_device.online IS '온라인 여부(집계) (예: true)';
COMMENT ON COLUMN hot.m_device.fault IS '고장 여부(집계) (예: false)';
COMMENT ON COLUMN hot.m_device.fault_code IS '현재 고장 코드';
COMMENT ON COLUMN hot.m_device.last_signals IS '최근 신호 값 {signal_code: {value, time}}';
COMMENT ON COLUMN hot.m_device.valid_from IS '설치일';
COMMENT ON COLUMN hot.m_device.valid_to IS '철거일';

CREATE TABLE hot.m_signal_def (
    class_code text NOT NULL,
    signal_code text NOT NULL,
    device_id text,
    name text,
    data_type text,
    unit text,
    enum_values text[],
    sample_period_s int,
    adapter text,
    address_template text,
    normal_min numeric,
    normal_max numeric,
    fault_rule_type text,
    fault_params jsonb,
    severity text,
    valid_from date,
    valid_to date,
    CONSTRAINT m_signal_def_uk UNIQUE NULLS NOT DISTINCT (class_code, signal_code, device_id)
);
COMMENT ON TABLE hot.m_signal_def IS '[M] 장비 유형 × 신호 1개 — 신호 사전·수집 경로·고장 규칙';
COMMENT ON COLUMN hot.m_signal_def.class_code IS '장비 유형(''*'' = 모든 장비 공통) (예: * / LIDAR / DEHUMID) (→ m_code.code)';
COMMENT ON COLUMN hot.m_signal_def.signal_code IS '신호 코드 (예: heartbeat / scan_state / humidity)';
COMMENT ON COLUMN hot.m_signal_def.device_id IS '특정 장비 예외일 때만(없으면 null) (→ m_device.device_id)';
COMMENT ON COLUMN hot.m_signal_def.name IS '신호 이름';
COMMENT ON COLUMN hot.m_signal_def.data_type IS 'num / bool / text / enum (예: bool / enum / num)';
COMMENT ON COLUMN hot.m_signal_def.unit IS '단위 (예: %RH)';
COMMENT ON COLUMN hot.m_signal_def.enum_values IS '열거값 (예: [IDLE,SCANNING,ERROR])';
COMMENT ON COLUMN hot.m_signal_def.sample_period_s IS '수집 주기(초) (예: 10)';
COMMENT ON COLUMN hot.m_signal_def.adapter IS '수집 경로(MQTT·Modbus·OPC-UA·REST·ISL TagId) (예: MQTT / Modbus)';
COMMENT ON COLUMN hot.m_signal_def.address_template IS '주소 규칙({device_id}.{topic} 등) (예: ot/sensor/{device_id}/state)';
COMMENT ON COLUMN hot.m_signal_def.normal_min IS '정상 하한 (예: 0)';
COMMENT ON COLUMN hot.m_signal_def.normal_max IS '정상 상한 (예: 60)';
COMMENT ON COLUMN hot.m_signal_def.fault_rule_type IS 'HEARTBEAT_TIMEOUT / FAULT_CODE / RANGE / ENUM / COMPOSITE (예: HEARTBEAT_TIMEOUT / ENUM / RANGE)';
COMMENT ON COLUMN hot.m_signal_def.fault_params IS '규칙 인자 (예: {timeout_s:60} / {fault_values:[ERROR]})';
COMMENT ON COLUMN hot.m_signal_def.severity IS '심각도 (예: HIGH)';
COMMENT ON COLUMN hot.m_signal_def.valid_from IS '유효 시작';
COMMENT ON COLUMN hot.m_signal_def.valid_to IS '유효 종료';

CREATE TABLE hot.m_file_store (
    store_id text NOT NULL,
    store_type text,
    base_uri text,
    path_rule text,
    retention_days int,
    owner text,
    PRIMARY KEY (store_id)
);
COMMENT ON TABLE hot.m_file_store IS '[M] 파일 저장소 1개(NAS 마운트·기본 경로)';
COMMENT ON COLUMN hot.m_file_store.store_id IS '저장소 ID (예: NAS_RAW)';
COMMENT ON COLUMN hot.m_file_store.store_type IS 'RAW_PCD / ARTIFACT / CAD / IMAGE (예: RAW_PCD)';
COMMENT ON COLUMN hot.m_file_store.base_uri IS '기본 경로 (예: nfs://nas01/ot/raw)';
COMMENT ON COLUMN hot.m_file_store.path_rule IS '하위 경로 규칙({shop}/{zone}/{yyyymmdd}/{scan_id}) (예: {shop}/{zone}/{yyyymmdd}/{scan_id})';
COMMENT ON COLUMN hot.m_file_store.retention_days IS '보존 일수 (예: 31)';
COMMENT ON COLUMN hot.m_file_store.owner IS '관리 주체';

CREATE TABLE hot.m_cad_file (
    cad_file_id text NOT NULL,
    hull_no text,
    block_no text,
    cad_level text,
    item_id text,
    store_id text,
    file_uri text,
    fmt text,
    rev_no text,
    received_at timestamptz,
    source_system text,
    valid bool,
    PRIMARY KEY (cad_file_id)
);
COMMENT ON TABLE hot.m_cad_file IS '[M] 경량 CAD 파일 1개';
COMMENT ON COLUMN hot.m_cad_file.cad_file_id IS 'CAD 파일 ID (예: CAD-5486-731-BK1A-A)';
COMMENT ON COLUMN hot.m_cad_file.hull_no IS '호선 (예: 5486) (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.m_cad_file.block_no IS '블록 (예: 731) (→ m_block.block_no)';
COMMENT ON COLUMN hot.m_cad_file.cad_level IS 'PART·ASSY / OUTFIT_BLOCK·OUTFIT_MODULE·OUTFIT_ITEM (예: PART)';
COMMENT ON COLUMN hot.m_cad_file.item_id IS '대응 번호(조립 PART_NO) (예: 5486-731-BK1A-A)';
COMMENT ON COLUMN hot.m_cad_file.store_id IS '저장소 (예: NAS_CAD) (→ m_file_store.store_id)';
COMMENT ON COLUMN hot.m_cad_file.file_uri IS '파일 경로(저장소 기준 상대) (예: 5486/731/BK1A-A.glb)';
COMMENT ON COLUMN hot.m_cad_file.fmt IS '3DP / FBX / GLB / OBJ (예: GLB)';
COMMENT ON COLUMN hot.m_cad_file.rev_no IS '리비전 (예: 01)';
COMMENT ON COLUMN hot.m_cad_file.received_at IS '수신 시각';
COMMENT ON COLUMN hot.m_cad_file.source_system IS '제공 경로(3DP·DAVIEW·WSD)';
COMMENT ON COLUMN hot.m_cad_file.valid IS '유효 여부';

CREATE TABLE hot.m_code (
    code_group text NOT NULL,
    code text NOT NULL,
    name text,
    in_scope bool,
    seq int,
    parent_code text,
    attrs jsonb,
    valid_from date,
    valid_to date,
    PRIMARY KEY (code_group, code)
);
COMMENT ON TABLE hot.m_code IS '[M] 코드 1개(코드 그룹 × 코드), 절점 정의 포함 · 레거시 복제';
COMMENT ON COLUMN hot.m_code.code_group IS 'TRADE / MP / STAGE / WSTG / ZONE / DEVICE_CLASS / CHECKPOINT (예: TRADE / WSTG / CHECKPOINT)';
COMMENT ON COLUMN hot.m_code.code IS '코드 값(절점이면 공종‖단계‖순서) (예: S / S6 / C‖J‖07)';
COMMENT ON COLUMN hot.m_code.name IS '명칭 (예: 조립 / 중조(S6) / 절단 완료(장비))';
COMMENT ON COLUMN hot.m_code.in_scope IS '내업 범위 여부 (예: true)';
COMMENT ON COLUMN hot.m_code.seq IS '순서(절점·Stage) (예: 7)';
COMMENT ON COLUMN hot.m_code.parent_code IS '상위 코드(절점의 공종·단계) (예: C‖J)';
COMMENT ON COLUMN hot.m_code.attrs IS '절점: judge_source·judge_rule·pct_at_cp / 송선: next_wstg (예: {next_wstg:G9} / {judge_source:LEGACY, judge_rule:''YPWC310M.CUT_MFG_FD 또는 YDEH060M.CUT_MFG_FD 입력''})';
COMMENT ON COLUMN hot.m_code.valid_from IS '유효 시작';
COMMENT ON COLUMN hot.m_code.valid_to IS '유효 종료';

CREATE TABLE hot.l_daily_plan (
    plan_date date NOT NULL,
    trade text NOT NULL,
    anchor_type text NOT NULL,
    anchor_id text NOT NULL,
    hull_no text,
    block_no text,
    location_id text,
    org_code text,
    priority int,
    wo_list text[],
    source_system text,
    loaded_at timestamptz,
    PRIMARY KEY (plan_date, trade, anchor_type, anchor_id)
);
COMMENT ON TABLE hot.l_daily_plan IS '[L] 작업일 × 공종 × 앵커 1개 — 일일 작업계획';
COMMENT ON COLUMN hot.l_daily_plan.plan_date IS '작업일 (예: 2022-08-16)';
COMMENT ON COLUMN hot.l_daily_plan.trade IS '공종 (예: S)';
COMMENT ON COLUMN hot.l_daily_plan.anchor_type IS 'WSTG / GDO / PAINT_ORDER / DWG (예: WSTG)';
COMMENT ON COLUMN hot.l_daily_plan.anchor_id IS '앵커 ID (예: 5486‖731‖S6G9)';
COMMENT ON COLUMN hot.l_daily_plan.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.l_daily_plan.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.l_daily_plan.location_id IS '작업 공장·구역·정반 (예: P502) (→ m_location.location_id)';
COMMENT ON COLUMN hot.l_daily_plan.org_code IS '작업 조직';
COMMENT ON COLUMN hot.l_daily_plan.priority IS '우선순위 (예: 1)';
COMMENT ON COLUMN hot.l_daily_plan.wo_list IS '연결된 W/O (예: [5486S731A000S6FG9A, …WG9A, …GG9A])';
COMMENT ON COLUMN hot.l_daily_plan.source_system IS '출처 화면·표 (예: ☆ 조립 일일계획(미확인))';
COMMENT ON COLUMN hot.l_daily_plan.loaded_at IS '적재 시각';

CREATE TABLE hot.l_work_unit (
    unit_id text NOT NULL,
    level text,
    parent_id text,
    act_no text,
    hull_no text,
    block_no text,
    trade text,
    mp text,
    stage text,
    wstg_code text,
    location_id text,
    plan_mh numeric,
    acc_rate numeric,
    plan_sd date,
    plan_fd date,
    actl_sd date,
    actl_fd date,
    attrs jsonb,
    mnt_ts timestamptz,
    PRIMARY KEY (unit_id)
);
COMMENT ON TABLE hot.l_work_unit IS '[L] 작업 단위 1개 — 액티비티 > 오더·공정 > W/O(±1개월 기준점)';
COMMENT ON COLUMN hot.l_work_unit.unit_id IS 'ACTIVITY: ZZACT / ORDER: AUFNR‖VORNR / WO: WORK_ORD_NO (예: 5486S731A000 / 000017254767‖0010 / 5486S731A000S6FG9A)';
COMMENT ON COLUMN hot.l_work_unit.level IS 'ACTIVITY / ORDER / WO (예: ACTIVITY / ORDER / WO)';
COMMENT ON COLUMN hot.l_work_unit.parent_id IS '상위 단위(WO→ORDER→ACTIVITY) (예: 5486S731A200 / 000017254767‖0010) (→ l_work_unit.unit_id)';
COMMENT ON COLUMN hot.l_work_unit.act_no IS '액티비티(W/O 앞 12자리 = GDO) (예: 5486S731A000 / 5486S731A200 / 5486W731GAG9 (GDO))';
COMMENT ON COLUMN hot.l_work_unit.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.l_work_unit.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.l_work_unit.trade IS '공종 (예: S / W / P)';
COMMENT ON COLUMN hot.l_work_unit.mp IS '공정 (예: G)';
COMMENT ON COLUMN hot.l_work_unit.stage IS 'Stage (예: B / A)';
COMMENT ON COLUMN hot.l_work_unit.wstg_code IS '송선(조립) (예: S6G9)';
COMMENT ON COLUMN hot.l_work_unit.location_id IS '공장·정반 (예: PBS / P502) (→ m_location.location_id)';
COMMENT ON COLUMN hot.l_work_unit.plan_mh IS '계획시수(212) 또는 표준시수(411 STD_MH) (예: 425.7 h / STD_MH(분) / 666)';
COMMENT ON COLUMN hot.l_work_unit.acc_rate IS '누계 진도율(W/O) (예: 100)';
COMMENT ON COLUMN hot.l_work_unit.plan_sd IS '계획 착수 (예: 2022-07-19 / 2022-08-08 / 2022-09-01)';
COMMENT ON COLUMN hot.l_work_unit.plan_fd IS '계획 완료 (예: 2022-08-26 / 2022-08-10 / 2022-09-07)';
COMMENT ON COLUMN hot.l_work_unit.actl_sd IS '실적 착수 (예: 2022-08-11 / 2022-09-09)';
COMMENT ON COLUMN hot.l_work_unit.actl_fd IS '실적 완료(마감 입력일) (예: 2022-08-16 / 2022-09-13)';
COMMENT ON COLUMN hot.l_work_unit.attrs IS 'W/O: 협력사·CARRYOVER / ORDER: 작업센터·DO 묶음 (예: {work_cntr:P62B0_01, oper_desc:''블록도장(도장)''})';
COMMENT ON COLUMN hot.l_work_unit.mnt_ts IS '수정일시(워터마크)';

CREATE TABLE hot.l_wo_daily (
    wo_no text NOT NULL,
    actl_date date NOT NULL,
    org_code text NOT NULL,
    daily_rate numeric,
    daily_mh numeric,
    rgsr_ts timestamptz,
    PRIMARY KEY (wo_no, actl_date, org_code)
);
COMMENT ON TABLE hot.l_wo_daily IS '[L] W/O × 실적일 × 작업 조직 1행 — 413 일자 시수 실적';
COMMENT ON COLUMN hot.l_wo_daily.wo_no IS 'W/O 번호(WO 레벨) (예: 5486W731GAG9001000) (→ l_work_unit.unit_id)';
COMMENT ON COLUMN hot.l_wo_daily.actl_date IS '실적일 (예: 2022-09-09 / 2022-09-12 / 2022-09-13)';
COMMENT ON COLUMN hot.l_wo_daily.org_code IS '작업 조직 (예: ADA1)';
COMMENT ON COLUMN hot.l_wo_daily.daily_rate IS '입력 진도율 (예: 30 / 40)';
COMMENT ON COLUMN hot.l_wo_daily.daily_mh IS '상응 시수 (예: 747 / 996)';
COMMENT ON COLUMN hot.l_wo_daily.rgsr_ts IS '등록일시(워터마크) (예: 2022-09-14)';

CREATE TABLE hot.l_assy_node (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    node_id text NOT NULL,
    node_level text,
    node_kind text,
    parent_node_id text,
    wstg_code text,
    next_wstg text,
    shape_group text,
    qty int,
    weight_kg numeric,
    cad_file_id text,
    plan_sd date,
    PRIMARY KEY (hull_no, block_no, node_id)
);
COMMENT ON TABLE hot.l_assy_node IS '[L] 블록 조립 하이러키 노드 1개(블록·조립품·부재)';
COMMENT ON COLUMN hot.l_assy_node.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.l_assy_node.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.l_assy_node.node_id IS '조립품 번호 또는 부재 번호 (예: 5486-731 / 5486-731-BK1A / 5486-731-BK1A-A)';
COMMENT ON COLUMN hot.l_assy_node.node_level IS 'BLOCK / 대조 / 중조 / 소조 / COMP / T-BAR / PART (예: BLOCK / 소조 / PART)';
COMMENT ON COLUMN hot.l_assy_node.node_kind IS 'ASSY / PART / STD (PART_NO_INDC) (예: ASSY / PART)';
COMMENT ON COLUMN hot.l_assy_node.parent_node_id IS '상위 노드 (예: 5486-731-…(S6 조립품) / 5486-731-BK1A) (→ l_assy_node.node_id)';
COMMENT ON COLUMN hot.l_assy_node.wstg_code IS '이 노드가 만들어지는 송선(WSTG) (예: G9 / C1) (→ m_code.code)';
COMMENT ON COLUMN hot.l_assy_node.next_wstg IS '다음 송선 (예: S6)';
COMMENT ON COLUMN hot.l_assy_node.shape_group IS '동일 형상 그룹(카운팅 단위)';
COMMENT ON COLUMN hot.l_assy_node.qty IS '같은 형상 개수(목표) (예: 1)';
COMMENT ON COLUMN hot.l_assy_node.weight_kg IS '중량 (예: 49,138 / 76.4)';
COMMENT ON COLUMN hot.l_assy_node.cad_file_id IS '대응 CAD (예: CAD-5486-731-BK1A-A) (→ m_cad_file.cad_file_id)';
COMMENT ON COLUMN hot.l_assy_node.plan_sd IS '월간계획 착수(210V)';

CREATE TABLE hot.l_cut_part (
    part_no text NOT NULL,
    hull_no text,
    block_no text,
    dwg_no text,
    cut_plan_date date,
    cut_bay text,
    cut_lot text,
    dest_wstg text,
    bend_flag bool,
    cut_done_ts timestamptz,
    group_ts timestamptz,
    sort_ts timestamptz,
    issue_ts timestamptz,
    inbound_ts timestamptz,
    cut_site text,
    PRIMARY KEY (part_no)
);
COMMENT ON TABLE hot.l_cut_part IS '[L] 가공 부재(절단 조각) 1개와 그 도면';
COMMENT ON COLUMN hot.l_cut_part.part_no IS '부재 번호(CHAR22) (예: 5486-731-000-K1 / 5486-731-000-B1)';
COMMENT ON COLUMN hot.l_cut_part.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.l_cut_part.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.l_cut_part.dwg_no IS '절단 도면(가공 앵커) (예: (310M 연결 확인 필요))';
COMMENT ON COLUMN hot.l_cut_part.cut_plan_date IS '절단 계획일(310M)';
COMMENT ON COLUMN hot.l_cut_part.cut_bay IS '절단 BAY';
COMMENT ON COLUMN hot.l_cut_part.cut_lot IS '동일 형상 절단 LOT';
COMMENT ON COLUMN hot.l_cut_part.dest_wstg IS '배송지 송선 (예: G9)';
COMMENT ON COLUMN hot.l_cut_part.bend_flag IS '곡가공 대상(510M) (예: false / true (F))';
COMMENT ON COLUMN hot.l_cut_part.cut_done_ts IS '절단 완료 (예: 2022-08-15 / (미입력))';
COMMENT ON COLUMN hot.l_cut_part.group_ts IS '모듬 등록';
COMMENT ON COLUMN hot.l_cut_part.sort_ts IS '부재 선별';
COMMENT ON COLUMN hot.l_cut_part.issue_ts IS '부재 불출(조립 투입) (예: 2022-08-15)';
COMMENT ON COLUMN hot.l_cut_part.inbound_ts IS '사외 절단 부재 반입(301M)';
COMMENT ON COLUMN hot.l_cut_part.cut_site IS '사내 / 사외 절단 (예: 사내)';

CREATE TABLE hot.l_outfit_item (
    rsnum text NOT NULL,
    rspos text NOT NULL,
    act_no text,
    hull_no text,
    block_no text,
    stage text,
    wstg_code text,
    matnr text,
    class_code text,
    do_no text,
    req_qty numeric,
    inst_qty numeric,
    inst_date date,
    issue_ts timestamptz,
    cad_file_id text,
    aedat date,
    PRIMARY KEY (rsnum, rspos)
);
COMMENT ON TABLE hot.l_outfit_item IS '[L] 의장 품목 1개(RSNUM‖RSPOS)';
COMMENT ON COLUMN hot.l_outfit_item.rsnum IS '예약 번호 (예: 0113910599)';
COMMENT ON COLUMN hot.l_outfit_item.rspos IS '예약 항목 (예: 0069)';
COMMENT ON COLUMN hot.l_outfit_item.act_no IS 'GDO(ZZACT) (예: 5486W731GAG9) (→ l_work_unit.act_no)';
COMMENT ON COLUMN hot.l_outfit_item.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.l_outfit_item.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.l_outfit_item.stage IS 'Stage (예: A)';
COMMENT ON COLUMN hot.l_outfit_item.wstg_code IS '송선(S·C·A) (예: G9)';
COMMENT ON COLUMN hot.l_outfit_item.matnr IS '품목 번호 (예: 5486TFTHR400A731)';
COMMENT ON COLUMN hot.l_outfit_item.class_code IS '분류 코드(지지류/본체) (예: TFTHR(지지류))';
COMMENT ON COLUMN hot.l_outfit_item.do_no IS 'DO 묶음(POTX1) (예: 5486W731AF2ZG9Z)';
COMMENT ON COLUMN hot.l_outfit_item.req_qty IS '소요 수량 (예: 1)';
COMMENT ON COLUMN hot.l_outfit_item.inst_qty IS '완설 수량 (예: 1)';
COMMENT ON COLUMN hot.l_outfit_item.inst_date IS '설치 입력일(W033) (예: 2022-09-21)';
COMMENT ON COLUMN hot.l_outfit_item.issue_ts IS '불출 시각(031·072)';
COMMENT ON COLUMN hot.l_outfit_item.cad_file_id IS '대응 CAD (→ m_cad_file.cad_file_id)';
COMMENT ON COLUMN hot.l_outfit_item.aedat IS '수정일(워터마크)';

CREATE TABLE hot.l_paint_element (
    wo_no text NOT NULL,
    elem_id text NOT NULL,
    elem_type text,
    seq int,
    done_date date,
    PRIMARY KEY (wo_no, elem_id)
);
COMMENT ON TABLE hot.l_paint_element IS '[L] 도장 W/O × 요소 1개(보조)';
COMMENT ON COLUMN hot.l_paint_element.wo_no IS '도장 W/O (→ l_work_unit.unit_id)';
COMMENT ON COLUMN hot.l_paint_element.elem_id IS '요소 ID';
COMMENT ON COLUMN hot.l_paint_element.elem_type IS '블라스팅·스프레이 n차·FINAL';
COMMENT ON COLUMN hot.l_paint_element.seq IS '수순';
COMMENT ON COLUMN hot.l_paint_element.done_date IS '완료일';

CREATE TABLE hot.l_bts_move (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    move_ts timestamptz NOT NULL,
    from_lot text,
    to_lot text,
    move_type text,
    PRIMARY KEY (hull_no, block_no, move_ts)
);
COMMENT ON TABLE hot.l_bts_move IS '[L] 블록 운반 1건(반입·반출)';
COMMENT ON COLUMN hot.l_bts_move.hull_no IS '호선 (예: 4506) (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.l_bts_move.block_no IS '블록 (예: 297) (→ m_block.block_no)';
COMMENT ON COLUMN hot.l_bts_move.move_ts IS '운반 시각 (예: (요청일 미입력))';
COMMENT ON COLUMN hot.l_bts_move.from_lot IS '출발 지번 (→ m_lot.lot_code)';
COMMENT ON COLUMN hot.l_bts_move.to_lot IS '도착 지번 (예: 1DS202) (→ m_lot.lot_code)';
COMMENT ON COLUMN hot.l_bts_move.move_type IS '반입/반출 (예: 반입)';

CREATE TABLE hot.s_scan_session (
    scan_id text NOT NULL,
    location_id text,
    scan_ts timestamptz,
    device_ids text[],
    raw_store_id text,
    raw_dir text,
    artifact_store_id text,
    artifact_dir text,
    artifacts jsonb,
    model_ver text,
    first_msg_ts timestamptz,
    last_msg_ts timestamptz,
    detection_cnt int,
    expected_cnt int,
    status text,
    PRIMARY KEY (scan_id)
);
COMMENT ON TABLE hot.s_scan_session IS '[S] 스캔 회차 1회(같은 scan_id 메시지들의 헤더)';
COMMENT ON COLUMN hot.s_scan_session.scan_id IS '스캔 ID (예: S-20220816-PBS5-0931)';
COMMENT ON COLUMN hot.s_scan_session.location_id IS '구역(ZONE) (예: PBS‖5) (→ m_location.location_id)';
COMMENT ON COLUMN hot.s_scan_session.scan_ts IS '스캔 시각 (예: 2022-08-16 09:31)';
COMMENT ON COLUMN hot.s_scan_session.device_ids IS '참여 라이다 (예: [LD-PBS-5-03, LD-PBS-5-04])';
COMMENT ON COLUMN hot.s_scan_session.raw_store_id IS '원시 PCD 저장소 (→ m_file_store.store_id)';
COMMENT ON COLUMN hot.s_scan_session.raw_dir IS '원시 PCD 디렉터리 (예: PBS/5/20220816/S-…-0931)';
COMMENT ON COLUMN hot.s_scan_session.artifact_store_id IS '산출물 저장소 (→ m_file_store.store_id)';
COMMENT ON COLUMN hot.s_scan_session.artifact_dir IS '산출물 디렉터리';
COMMENT ON COLUMN hot.s_scan_session.artifacts IS '[{type, uri, checksum}]';
COMMENT ON COLUMN hot.s_scan_session.model_ver IS '추론 모델 버전';
COMMENT ON COLUMN hot.s_scan_session.first_msg_ts IS '첫 메시지 수신';
COMMENT ON COLUMN hot.s_scan_session.last_msg_ts IS '마지막 메시지 수신';
COMMENT ON COLUMN hot.s_scan_session.detection_cnt IS '받은 인식 객체 수 (예: 2)';
COMMENT ON COLUMN hot.s_scan_session.expected_cnt IS '예고된 객체 수 (예: (종료 메시지 없음))';
COMMENT ON COLUMN hot.s_scan_session.status IS 'OPEN / CLOSED / FAILED (예: OPEN → CLOSED(유예 10분))';

CREATE TABLE hot.s_scan_detection (
    scan_id text NOT NULL,
    object_key text NOT NULL,
    event_id text,
    received_ts timestamptz,
    object_type text,
    hull_no text,
    block_no text,
    item_id text,
    anchor_type text,
    anchor_id text,
    cad_file_id text,
    match_conf numeric,
    pose jsonb,
    bbox jsonb,
    progress_est numeric,
    count_est int,
    presence bool,
    stage_hint text,
    event_type text,
    segment_uri text,
    model_ver text,
    ocr_text text,
    judged bool,
    judged_ts timestamptz,
    progress_ref text,
    PRIMARY KEY (scan_id, object_key)
);
COMMENT ON TABLE hot.s_scan_detection IS '[S] 인식 객체 1개 = 실적 메시지 1건';
COMMENT ON COLUMN hot.s_scan_detection.scan_id IS '스캔 회차 (예: S-20220816-PBS5-0931) (→ s_scan_session.scan_id)';
COMMENT ON COLUMN hot.s_scan_detection.object_key IS '객체 키(object_id, 없으면 수신 순번) (예: 5486-731-BL34A / 5486-731-BL34B)';
COMMENT ON COLUMN hot.s_scan_detection.event_id IS '원문 메시지 ID, 1:1 (예: E-…-001 / E-…-002) (→ ts.sensor_event.event_id)';
COMMENT ON COLUMN hot.s_scan_detection.received_ts IS '수신 시각';
COMMENT ON COLUMN hot.s_scan_detection.object_type IS 'BLOCK / ASSY / PART / OUTFIT_ITEM / UNKNOWN (예: ASSY)';
COMMENT ON COLUMN hot.s_scan_detection.hull_no IS '인식된 호선 (예: 5486) (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.s_scan_detection.block_no IS '인식된 블록 (예: 731) (→ m_block.block_no)';
COMMENT ON COLUMN hot.s_scan_detection.item_id IS '조립품·부재·품목 ID(object_type에 따라 l_assy_node / l_cut_part / l_outfit_item) (예: 5486-731-BL34A / 5486-731-BL34B) (→ l_assy_node.node_id)';
COMMENT ON COLUMN hot.s_scan_detection.anchor_type IS 'WSTG / GDO / DWG / PAINT_ORDER (예: WSTG)';
COMMENT ON COLUMN hot.s_scan_detection.anchor_id IS '매칭된 앵커 (예: 5486‖731‖S6G9) (→ p_progress.anchor_id)';
COMMENT ON COLUMN hot.s_scan_detection.cad_file_id IS '매칭에 쓴 CAD (예: CAD-5486-731-BL34A) (→ m_cad_file.cad_file_id)';
COMMENT ON COLUMN hot.s_scan_detection.match_conf IS 'CAD 매칭 신뢰도(0~1) (예: 0.91 / 0.87)';
COMMENT ON COLUMN hot.s_scan_detection.pose IS '{x,y,z,yaw} 또는 {matrix} (예: {x:12.3, y:4.1, yaw:92})';
COMMENT ON COLUMN hot.s_scan_detection.bbox IS '{l,w,h}';
COMMENT ON COLUMN hot.s_scan_detection.progress_est IS '필드 추정 진행률 (예: 0.80 / 0.55)';
COMMENT ON COLUMN hot.s_scan_detection.count_est IS '인식 개수 (예: 1)';
COMMENT ON COLUMN hot.s_scan_detection.presence IS '존재 여부(반출 판정)';
COMMENT ON COLUMN hot.s_scan_detection.stage_hint IS '필드 스테이지';
COMMENT ON COLUMN hot.s_scan_detection.event_type IS 'START / PROGRESS / COMPLETE';
COMMENT ON COLUMN hot.s_scan_detection.segment_uri IS '객체 세그먼트 PCD 경로';
COMMENT ON COLUMN hot.s_scan_detection.model_ver IS '모델 버전';
COMMENT ON COLUMN hot.s_scan_detection.ocr_text IS 'OCR 판독 문자';
COMMENT ON COLUMN hot.s_scan_detection.judged IS '판별 반영 여부 (예: true)';
COMMENT ON COLUMN hot.s_scan_detection.judged_ts IS '판별 시각';
COMMENT ON COLUMN hot.s_scan_detection.progress_ref IS '반영된 p_progress 키(hull‖block‖anchor_type‖anchor_id) (예: 5486‖731‖WSTG‖5486‖731‖S6G9)';

CREATE TABLE ts.device_signal (
    time timestamptz NOT NULL,
    device_id text NOT NULL,
    signal_code text NOT NULL,
    kind text,
    value_num numeric,
    value_text text,
    quality text,
    PRIMARY KEY (time, device_id, signal_code)
);
COMMENT ON TABLE ts.device_signal IS '[T] 장비 × 신호 × 시각 1점(하이퍼테이블)';
COMMENT ON COLUMN ts.device_signal.time IS '시각(파티션 키) (예: 2022-08-16 09:30:00 / 2022-08-16 09:40:12)';
COMMENT ON COLUMN ts.device_signal.device_id IS '장비(segmentby) (예: LD-PBS-5-03) (→ m_device.device_id)';
COMMENT ON COLUMN ts.device_signal.signal_code IS '신호 코드(사건이면 규칙 코드) (예: scan_state / heartbeat) (→ m_signal_def.signal_code)';
COMMENT ON COLUMN ts.device_signal.kind IS 'SIGNAL / EVENT (예: SIGNAL / EVENT)';
COMMENT ON COLUMN ts.device_signal.value_num IS '숫자 값';
COMMENT ON COLUMN ts.device_signal.value_text IS '문자 값(ONLINE·OFFLINE·FAULT_ON …) (예: SCANNING / OFFLINE)';
COMMENT ON COLUMN ts.device_signal.quality IS '품질';

CREATE TABLE ts.sensor_event (
    time timestamptz NOT NULL,
    event_id text NOT NULL,
    scan_id text,
    object_key text,
    zone text,
    stage text,
    location_id text,
    hull_no text,
    block_no text,
    object_type text,
    event_type text,
    source_system text,
    idempotency_key text,
    raw_payload jsonb,
    PRIMARY KEY (time, event_id)
);
COMMENT ON TABLE ts.sensor_event IS '[T] 실적 메시지 원문 1건(하이퍼테이블)';
COMMENT ON COLUMN ts.sensor_event.time IS '발생 시각(occurred_at) (예: 2022-08-16 09:33:10)';
COMMENT ON COLUMN ts.sensor_event.event_id IS '메시지 ID (예: E-…-001)';
COMMENT ON COLUMN ts.sensor_event.scan_id IS '스캔 회차 (예: S-20220816-PBS5-0931) (→ s_scan_session.scan_id)';
COMMENT ON COLUMN ts.sensor_event.object_key IS '객체 키 (예: 5486-731-BL34A)';
COMMENT ON COLUMN ts.sensor_event.zone IS '권역 (예: ASSEMBLY)';
COMMENT ON COLUMN ts.sensor_event.stage IS '스테이지 (예: FITTING)';
COMMENT ON COLUMN ts.sensor_event.location_id IS '구역 (예: PBS‖5) (→ m_location.location_id)';
COMMENT ON COLUMN ts.sensor_event.hull_no IS '호선 (예: 5486) (→ m_hull.hull_no)';
COMMENT ON COLUMN ts.sensor_event.block_no IS '블록 (예: 731) (→ m_block.block_no)';
COMMENT ON COLUMN ts.sensor_event.object_type IS 'BLOCK / ASSY / PART / OUTFIT_ITEM (예: ASSY)';
COMMENT ON COLUMN ts.sensor_event.event_type IS 'START / PROGRESS / COMPLETE (예: PROGRESS)';
COMMENT ON COLUMN ts.sensor_event.source_system IS '발생 시스템';
COMMENT ON COLUMN ts.sensor_event.idempotency_key IS 'scan_id‖object_key‖occurred_at (예: S-…-0931‖5486-731-BL34A‖20220816093310)';
COMMENT ON COLUMN ts.sensor_event.raw_payload IS '원문';

CREATE TABLE hot.p_progress (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    anchor_type text NOT NULL,
    anchor_id text NOT NULL,
    as_of date NOT NULL,
    state text,
    auto_pct numeric,
    pct_basis text,
    legacy_pct numeric,
    gap_pct numeric,
    planned_qty numeric,
    done_qty numeric,
    last_cp text,
    cp_history jsonb,
    evidence jsonb,
    location_id text,
    first_detect_ts timestamptz,
    last_detect_ts timestamptz,
    confirmed_by text,
    PRIMARY KEY (hull_no, block_no, anchor_type, anchor_id, as_of)
);
COMMENT ON TABLE hot.p_progress IS '[P] 앵커 × 기준일 1행 · 레거시 복제';
COMMENT ON COLUMN hot.p_progress.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.p_progress.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.p_progress.anchor_type IS 'DWG / WSTG / GDO / PAINT_ORDER (예: WSTG / GDO / PAINT_ORDER)';
COMMENT ON COLUMN hot.p_progress.anchor_id IS '앵커 ID (예: 5486‖731‖S6G9 / 5486W731GAG9 / 5486‖731‖P)';
COMMENT ON COLUMN hot.p_progress.as_of IS '기준일 (예: 2022-08-16 / 2022-09-21 / 2022-09-30)';
COMMENT ON COLUMN hot.p_progress.state IS 'notDue / inProgress / done / confirmed (예: inProgress / done)';
COMMENT ON COLUMN hot.p_progress.auto_pct IS '자동 산출 퍼센트 (예: ☆ 64.3 (인식 9 ÷ 목표 14) / 78.1 (설치 25 ÷ 소요 32, W033) / (표본에 413 없음) = Σ413 ÷ (570+666))';
COMMENT ON COLUMN hot.p_progress.pct_basis IS 'COUNT / INSTALL / MH / DWG (예: COUNT / INSTALL / MH)';
COMMENT ON COLUMN hot.p_progress.legacy_pct IS '레거시 진도율(비교값) (예: 100 (W/O ACCM_PRGS_RATE, 완료 08-16) / 100 (W/O 413 누계 30+40+30) / 100 (W/O 완료 09-27·09-30))';
COMMENT ON COLUMN hot.p_progress.gap_pct IS '자동 − 레거시 (예: −35.7 / −21.9)';
COMMENT ON COLUMN hot.p_progress.planned_qty IS '목표 (예: 14 (050M S6→G9 조립품) / 32 / 1,236 MH)';
COMMENT ON COLUMN hot.p_progress.done_qty IS '달성 (예: ☆ 9 / 25)';
COMMENT ON COLUMN hot.p_progress.last_cp IS '마지막 도달 절점(CHECKPOINT) (예: C‖J‖10 부재 불출) (→ m_code.code)';
COMMENT ON COLUMN hot.p_progress.cp_history IS '[{cp, reached_at, source, ref, conf, confirmed}] (예: [{cp:C‖J‖07 절단 완료, reached_at:2022-08-15, source:LEGACY, ref:YDEH060M.CUT_MFG_FD}, {cp:C‖J‖10, reached_at:2022-08-15, ref:CUT_ISS_FD}])';
COMMENT ON COLUMN hot.p_progress.evidence IS '근거 요약 (예: [{scan_id:S-…-0931, object_key:5486-731-BL34A}, …])';
COMMENT ON COLUMN hot.p_progress.location_id IS '수행 공장·구역·정반 (예: P502 / PBS / NO2) (→ m_location.location_id)';
COMMENT ON COLUMN hot.p_progress.first_detect_ts IS '첫 인식';
COMMENT ON COLUMN hot.p_progress.last_detect_ts IS '마지막 인식';
COMMENT ON COLUMN hot.p_progress.confirmed_by IS '확정 Key-In (→ p_action.action_id)';

CREATE TABLE hot.p_progress_block (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    trade text NOT NULL,
    stage text NOT NULL,
    as_of date NOT NULL,
    auto_pct numeric,
    legacy_pct numeric,
    weight_basis text,
    anchor_cnt int,
    open_action_cnt int,
    bts_lot text,
    sensed_location_id text,
    PRIMARY KEY (hull_no, block_no, trade, stage, as_of)
);
COMMENT ON TABLE hot.p_progress_block IS '[P] 블록 × 공종 × 단계 × 기준일 롤업 · 레거시 복제';
COMMENT ON COLUMN hot.p_progress_block.hull_no IS '호선 (예: 5486) (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.p_progress_block.block_no IS '블록 (예: 731) (→ m_block.block_no)';
COMMENT ON COLUMN hot.p_progress_block.trade IS '공종 (예: S / W)';
COMMENT ON COLUMN hot.p_progress_block.stage IS '단계 (예: B / A)';
COMMENT ON COLUMN hot.p_progress_block.as_of IS '기준일 (예: 2022-08-16 / 2022-09-21)';
COMMENT ON COLUMN hot.p_progress_block.auto_pct IS '자동 실적률 (예: ☆ 71.2 (송선 11개 STD_MH 가중) / 78.1)';
COMMENT ON COLUMN hot.p_progress_block.legacy_pct IS '레거시 실적률 (예: 100)';
COMMENT ON COLUMN hot.p_progress_block.weight_basis IS '가중 기준 (예: W/O STD_MH / W/O 시수)';
COMMENT ON COLUMN hot.p_progress_block.anchor_cnt IS '앵커 수 (예: 11 / 1)';
COMMENT ON COLUMN hot.p_progress_block.open_action_cnt IS '미해결 알림 수 (예: 1)';
COMMENT ON COLUMN hot.p_progress_block.bts_lot IS 'BTS 위치 (→ m_lot.lot_code)';
COMMENT ON COLUMN hot.p_progress_block.sensed_location_id IS '센서 인식 구역 (예: PBS‖5) (→ m_location.location_id)';

CREATE TABLE hot.p_item_state (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    item_type text NOT NULL,
    item_id text NOT NULL,
    anchor_id text,
    state text,
    detect_ts timestamptz,
    legacy_ts timestamptz,
    scan_id text,
    detection_seq int,
    PRIMARY KEY (hull_no, block_no, item_type, item_id)
);
COMMENT ON TABLE hot.p_item_state IS '[P] 조립품·부재·의장 품목 1개의 현재 상태 · 레거시 복제';
COMMENT ON COLUMN hot.p_item_state.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.p_item_state.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.p_item_state.item_type IS 'ASSY / PART / OUTFIT_ITEM (예: ASSY / OUTFIT_ITEM)';
COMMENT ON COLUMN hot.p_item_state.item_id IS 'node_id 또는 RSNUM+RSPOS (예: 5486-731-BL34A / 0113910599‖0069)';
COMMENT ON COLUMN hot.p_item_state.anchor_id IS '소속 앵커 (예: 5486‖731‖S6G9 / 5486W731GAG9) (→ p_progress.anchor_id)';
COMMENT ON COLUMN hot.p_item_state.state IS 'notDue / issued / detected / installed / carried (예: detected / installed)';
COMMENT ON COLUMN hot.p_item_state.detect_ts IS '센서 인식 시각 (예: ☆ 2022-08-16 09:33)';
COMMENT ON COLUMN hot.p_item_state.legacy_ts IS '레거시 입력 시각 (예: 2022-09-21)';
COMMENT ON COLUMN hot.p_item_state.scan_id IS '인식 회차 (예: S-…-0931) (→ s_scan_session.scan_id)';
COMMENT ON COLUMN hot.p_item_state.detection_seq IS '인식 객체 (예: 5486-731-BL34A) (→ s_scan_detection.object_key)';

CREATE TABLE hot.p_action (
    action_id bigint NOT NULL,
    action_type text,
    hull_no text,
    block_no text,
    anchor_id text,
    sub_type text,
    detail text,
    value_pct numeric,
    cp_code text,
    reason_code text,
    user_id text,
    raised_at timestamptz,
    status text,
    resolved_by bigint,
    resolved_at timestamptz,
    PRIMARY KEY (action_id)
);
COMMENT ON TABLE hot.p_action IS '[P] 알림 1건 또는 Key-In 1건 · 레거시 복제';
COMMENT ON COLUMN hot.p_action.action_id IS 'ID (예: 1001 / 1002)';
COMMENT ON COLUMN hot.p_action.action_type IS 'EXCEPTION / KEYIN (예: EXCEPTION / KEYIN)';
COMMENT ON COLUMN hot.p_action.hull_no IS '호선 (→ m_hull.hull_no)';
COMMENT ON COLUMN hot.p_action.block_no IS '블록 (→ m_block.block_no)';
COMMENT ON COLUMN hot.p_action.anchor_id IS '앵커 (예: 5486W731GAG9) (→ p_progress.anchor_id)';
COMMENT ON COLUMN hot.p_action.sub_type IS 'UNMATCHED·ORDER_REVERSED·GAP·NO_PLAN / PCT·CHECKPOINT·CORRECTION (예: GAP / CORRECTION)';
COMMENT ON COLUMN hot.p_action.detail IS '내용 (예: W/O 누계 100%(09-13)인데 품목 설치 25/32 — 7개 미설치 / 미설치 7개는 A Stage 이월(carried) 처리)';
COMMENT ON COLUMN hot.p_action.value_pct IS '입력 퍼센트(Key-In)';
COMMENT ON COLUMN hot.p_action.cp_code IS '확정 절점(Key-In) (→ m_code.code)';
COMMENT ON COLUMN hot.p_action.reason_code IS '사유';
COMMENT ON COLUMN hot.p_action.user_id IS '입력·처리자 (예: 관리자)';
COMMENT ON COLUMN hot.p_action.raised_at IS '발생·입력 시각 (예: 2022-09-21)';
COMMENT ON COLUMN hot.p_action.status IS 'OPEN·RESOLVED / 입력·임시저장·확정 (예: OPEN / 확정)';
COMMENT ON COLUMN hot.p_action.resolved_by IS '해결한 Key-In ID (→ p_action.action_id)';
COMMENT ON COLUMN hot.p_action.resolved_at IS '해결 시각';

-- 하이퍼테이블 (TimescaleDB)
-- SELECT create_hypertable('ts.device_signal', 'time', chunk_time_interval => interval '6 hours');
-- SELECT create_hypertable('ts.sensor_event', 'time');

-- FK 제약 (참조 대상 PK 와 일치하는 관계만)
ALTER TABLE hot.m_block ADD CONSTRAINT fk_m_block_0 FOREIGN KEY (hull_no) REFERENCES hot.m_hull (hull_no);
ALTER TABLE hot.m_block ADD CONSTRAINT fk_m_block_1 FOREIGN KEY (bts_lot) REFERENCES hot.m_lot (lot_code);
ALTER TABLE hot.m_block ADD CONSTRAINT fk_m_block_2 FOREIGN KEY (sensed_location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.m_location ADD CONSTRAINT fk_m_location_3 FOREIGN KEY (parent_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.m_location ADD CONSTRAINT fk_m_location_4 FOREIGN KEY (lot_code) REFERENCES hot.m_lot (lot_code);
ALTER TABLE hot.m_lot ADD CONSTRAINT fk_m_lot_5 FOREIGN KEY (location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.m_device ADD CONSTRAINT fk_m_device_6 FOREIGN KEY (parent_device_id) REFERENCES hot.m_device (device_id);
ALTER TABLE hot.m_device ADD CONSTRAINT fk_m_device_7 FOREIGN KEY (location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.m_signal_def ADD CONSTRAINT fk_m_signal_def_8 FOREIGN KEY (device_id) REFERENCES hot.m_device (device_id);
ALTER TABLE hot.m_cad_file ADD CONSTRAINT fk_m_cad_file_9 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.m_cad_file ADD CONSTRAINT fk_m_cad_file_10 FOREIGN KEY (store_id) REFERENCES hot.m_file_store (store_id);
ALTER TABLE hot.l_daily_plan ADD CONSTRAINT fk_l_daily_plan_11 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.l_daily_plan ADD CONSTRAINT fk_l_daily_plan_12 FOREIGN KEY (location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.l_work_unit ADD CONSTRAINT fk_l_work_unit_13 FOREIGN KEY (parent_id) REFERENCES hot.l_work_unit (unit_id);
ALTER TABLE hot.l_work_unit ADD CONSTRAINT fk_l_work_unit_14 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.l_work_unit ADD CONSTRAINT fk_l_work_unit_15 FOREIGN KEY (location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.l_wo_daily ADD CONSTRAINT fk_l_wo_daily_16 FOREIGN KEY (wo_no) REFERENCES hot.l_work_unit (unit_id);
ALTER TABLE hot.l_assy_node ADD CONSTRAINT fk_l_assy_node_17 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.l_assy_node ADD CONSTRAINT fk_l_assy_node_18 FOREIGN KEY (hull_no, block_no, parent_node_id) REFERENCES hot.l_assy_node (hull_no, block_no, node_id);
ALTER TABLE hot.l_assy_node ADD CONSTRAINT fk_l_assy_node_19 FOREIGN KEY (cad_file_id) REFERENCES hot.m_cad_file (cad_file_id);
ALTER TABLE hot.l_cut_part ADD CONSTRAINT fk_l_cut_part_20 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.l_outfit_item ADD CONSTRAINT fk_l_outfit_item_21 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.l_outfit_item ADD CONSTRAINT fk_l_outfit_item_22 FOREIGN KEY (cad_file_id) REFERENCES hot.m_cad_file (cad_file_id);
ALTER TABLE hot.l_paint_element ADD CONSTRAINT fk_l_paint_element_23 FOREIGN KEY (wo_no) REFERENCES hot.l_work_unit (unit_id);
ALTER TABLE hot.l_bts_move ADD CONSTRAINT fk_l_bts_move_24 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.l_bts_move ADD CONSTRAINT fk_l_bts_move_25 FOREIGN KEY (from_lot) REFERENCES hot.m_lot (lot_code);
ALTER TABLE hot.l_bts_move ADD CONSTRAINT fk_l_bts_move_26 FOREIGN KEY (to_lot) REFERENCES hot.m_lot (lot_code);
ALTER TABLE hot.s_scan_session ADD CONSTRAINT fk_s_scan_session_27 FOREIGN KEY (location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.s_scan_session ADD CONSTRAINT fk_s_scan_session_28 FOREIGN KEY (raw_store_id) REFERENCES hot.m_file_store (store_id);
ALTER TABLE hot.s_scan_session ADD CONSTRAINT fk_s_scan_session_29 FOREIGN KEY (artifact_store_id) REFERENCES hot.m_file_store (store_id);
ALTER TABLE hot.s_scan_detection ADD CONSTRAINT fk_s_scan_detection_30 FOREIGN KEY (scan_id) REFERENCES hot.s_scan_session (scan_id);
ALTER TABLE hot.s_scan_detection ADD CONSTRAINT fk_s_scan_detection_31 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.s_scan_detection ADD CONSTRAINT fk_s_scan_detection_32 FOREIGN KEY (cad_file_id) REFERENCES hot.m_cad_file (cad_file_id);
ALTER TABLE ts.device_signal ADD CONSTRAINT fk_ts_device_signal_33 FOREIGN KEY (device_id) REFERENCES hot.m_device (device_id);
ALTER TABLE ts.sensor_event ADD CONSTRAINT fk_ts_sensor_event_34 FOREIGN KEY (scan_id) REFERENCES hot.s_scan_session (scan_id);
ALTER TABLE ts.sensor_event ADD CONSTRAINT fk_ts_sensor_event_35 FOREIGN KEY (location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE ts.sensor_event ADD CONSTRAINT fk_ts_sensor_event_36 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.p_progress ADD CONSTRAINT fk_p_progress_37 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.p_progress ADD CONSTRAINT fk_p_progress_38 FOREIGN KEY (location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.p_progress_block ADD CONSTRAINT fk_p_progress_block_39 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.p_progress_block ADD CONSTRAINT fk_p_progress_block_40 FOREIGN KEY (bts_lot) REFERENCES hot.m_lot (lot_code);
ALTER TABLE hot.p_progress_block ADD CONSTRAINT fk_p_progress_block_41 FOREIGN KEY (sensed_location_id) REFERENCES hot.m_location (location_id);
ALTER TABLE hot.p_item_state ADD CONSTRAINT fk_p_item_state_42 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.p_item_state ADD CONSTRAINT fk_p_item_state_43 FOREIGN KEY (scan_id) REFERENCES hot.s_scan_session (scan_id);
ALTER TABLE hot.p_action ADD CONSTRAINT fk_p_action_44 FOREIGN KEY (hull_no, block_no) REFERENCES hot.m_block (hull_no, block_no);
ALTER TABLE hot.p_action ADD CONSTRAINT fk_p_action_45 FOREIGN KEY (resolved_by) REFERENCES hot.p_action (action_id);

-- 논리 참조만 (PK 불일치·형 불일치·다형 참조라 제약 없음)
--   m_device(class_code) → m_code(code)
--   m_signal_def(class_code) → m_code(code)
--   l_assy_node(wstg_code) → m_code(code)
--   l_outfit_item(act_no) → l_work_unit(act_no)
--   s_scan_detection(event_id) → ts.sensor_event(event_id)
--   s_scan_detection(item_id) → l_assy_node(node_id)
--   s_scan_detection(anchor_id) → p_progress(anchor_id)
--   ts.device_signal(signal_code) → m_signal_def(signal_code)
--   p_progress(last_cp) → m_code(code)
--   p_progress(confirmed_by) → p_action(action_id)
--   p_item_state(anchor_id) → p_progress(anchor_id)
--   p_item_state(detection_seq) → s_scan_detection(object_key)
--   p_action(anchor_id) → p_progress(anchor_id)
--   p_action(cp_code) → m_code(code)
