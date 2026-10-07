-- [svc_*] 권역별 RDB. 네 권역이 같은 표 모양을 갖는다.
-- 채우는 규칙은 zone-service 의 라우트 설정(application.yml)에 있다 — 표를 바꾸면 거기도 같이 바꾼다.
--
-- 모든 표에 공통으로 두는 세 컬럼:
--   src_event_time  : 원천 발생 시각 (occurred_at)
--   src_received_at : Provider 수신 시각 (tsdb 의 received_at)
--   applied_at      : 이 표에 반영된 DB 시각 — applied_at - src_received_at 이 CDC 지연이다.
--                     DB 시계 하나로 재므로 서버가 갈라져도 시계 편차가 섞이지 않는다.
DO $$
DECLARE s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['svc_asm','svc_oft','svc_pnt','svc_mch'] LOOP

    -- 장비별 최신 상태 1행
    EXECUTE format($f$
      CREATE TABLE %I.device_status_current (
        device_id             text PRIMARY KEY,
        status                text NOT NULL,
        error_code            text,
        last_heartbeat_at     timestamptz,
        scan_rate_pts_per_sec integer,
        temperature_c         real,
        connectivity_rssi     smallint,
        fov_mode              text,
        src_event_time        timestamptz NOT NULL,
        src_received_at       timestamptz NOT NULL,
        applied_at            timestamptz NOT NULL
      )$f$, s);
    EXECUTE format('CREATE INDEX ON %I.device_status_current (applied_at)', s);

    -- 상태가 바뀐 순간만
    EXECUTE format($f$
      CREATE TABLE %I.device_status_change (
        device_id       text        NOT NULL,
        changed_at      timestamptz NOT NULL,
        status          text        NOT NULL,
        error_code      text,
        src_received_at timestamptz NOT NULL,
        applied_at      timestamptz NOT NULL,
        PRIMARY KEY (device_id, changed_at)
      )$f$, s);

    -- 스캔 1건의 진행 (START → PROGRESS … → COMPLETE)
    EXECUTE format($f$
      CREATE TABLE %I.scan (
        scan_id             uuid PRIMARY KEY,
        device_id           text        NOT NULL,
        hull_no             text        NOT NULL,
        block_id            text        NOT NULL,
        first_event_at      timestamptz NOT NULL,
        last_event_at       timestamptz NOT NULL,
        last_event_type     text        NOT NULL,
        block_progress_rate real,
        match_confidence    real,
        reference_cad_id    text,
        model_version       text,
        completed_at        timestamptz,
        src_received_at     timestamptz NOT NULL,
        applied_at          timestamptz NOT NULL
      )$f$, s);
    EXECUTE format('CREATE INDEX ON %I.scan (hull_no, block_id)', s);

    -- 실적 판별 대상 (COMPLETE 된 스캔). judged_status 는 판별 로직이 채운다 (2단계)
    EXECUTE format($f$
      CREATE TABLE %I.actual_result (
        hull_no             text        NOT NULL,
        block_id            text        NOT NULL,
        scan_id             uuid        NOT NULL,
        device_id           text        NOT NULL,
        completed_at        timestamptz NOT NULL,
        block_progress_rate real,
        match_confidence    real,
        reference_cad_id    text,
        model_version       text,
        judged_status       text        NOT NULL DEFAULT 'PENDING',
        src_received_at     timestamptz NOT NULL,
        applied_at          timestamptz NOT NULL,
        PRIMARY KEY (hull_no, block_id, scan_id)
      )$f$, s);

    -- 산출물 위치
    EXECUTE format($f$
      CREATE TABLE %I.artifact (
        scan_id               uuid        NOT NULL,
        artifact_type         text        NOT NULL,
        segment_key           text        NOT NULL,
        device_id             text        NOT NULL,
        hull_no               text        NOT NULL,
        block_id              text        NOT NULL,
        storage_uri           text,
        file_size_bytes       bigint,
        checksum              text,
        transformation_matrix real[],
        produced_by_device_id text,
        model_version         text,
        src_event_time        timestamptz NOT NULL,
        src_received_at       timestamptz NOT NULL,
        applied_at            timestamptz NOT NULL,
        PRIMARY KEY (scan_id, artifact_type, segment_key)
      )$f$, s);
    EXECUTE format('CREATE INDEX ON %I.artifact (applied_at)', s);

  END LOOP;
END $$;
