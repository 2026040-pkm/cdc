# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Vite + React + TypeScript, `src/nats-tag-stream/web`. 백엔드(`nats-tag-stream`, Spring Boot :64080)의 WebSocket `/ws/tags` 를 받는다. 개발은 `npm run dev` 별도 서버.

## Users

조선소 LiDAR 현장 운영자. 조립(assembly1) · 의장(outfitting1) 공장의 bay 에 설치된 LiDAR 장비 350대가 살아 있는지,
어느 bay 에서 공정 실적(배재 · 취부 · 용접 · 검사 · 배관 · 배선)이 올라오는지를 실시간으로 지켜본다.

## Product Purpose

ISL Engine 이 NATS 로 흘려보내는 태그 변경(상태 1초 · 실적 1분 · 산출물 1분)을 사람이 읽을 수 있는 현장 관제 화면으로 바꾼다.
성공: 장비 이상(OFFLINE · error_code · 수신 끊김)과 bay 별 실적 흐름을 몇 초 안에 알아챈다.

## Operating Context

- 데이터: `{"type":"tags","receivedAt","count","tags":[{tagId, device, artifactType, topicKey, channel, changedAt, value}]}` 이 약 1초마다 한 프레임(필터 없이 400~490건 · 약 300KB).
- channel 셋: status(장비 상태: status, error_code, temperature_c, scan_rate_pts_per_sec, connectivity_rssi, fov_mode),
  actual(실적: stage, event_type START/COMPLETE, hull_no, block_id, block_progress_rate, match_confidence),
  artifact(산출물: REGISTERED_PCD · TRANSFORMATION_MATRIX · SEGMENTED_PCD, file_size_bytes, storage_uri).
- 장비 id 규칙 `LDR-GJ-{A|O}1B{bay}-{nn}`: 조립 7 bay × 30대, 의장 7 bay × 20대.
- 접속 전 데이터는 없다(NATS 일반 구독). 끊기면 다시 붙어야 한다.

## Capabilities and Constraints

- 서버 필터: `?channel=` · `?device=` (장비 id 앞부분). 화면 필터는 클라이언트에서 해도 된다.
- `changedAt` 은 장비 쪽 occurred_at 이다. 도착 순서가 아니다.
- 지금 데이터는 시뮬레이터(lidar-sim) 값이다. 실제 현장 수치처럼 표기하지 않는다.

## Brand Commitments

팀 문서(`latest/docs/*.html`)가 쓰는 한화 오렌지 계열 팔레트(`--hw #F37321`), Pretendard / D2Coding, 라이트 · 다크 양쪽.
보라 계열과 이모지는 쓰지 않는다.

## Product Principles

- 이상이 먼저 보인다. 정상 350대는 조용하고, 벗어난 것만 눈에 띈다.
- 공장 · bay 라는 현장의 공간 구조로 읽힌다.
- 숫자는 원문으로 확인할 수 있다 — 어느 칸이든 눌러 raw_payload 를 본다.
