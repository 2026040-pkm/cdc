# nats-tag-stream

ISL Engine 이 NATS 로 뿌리는 태그 변경을 WebSocket 으로 실시간 중계한다.

```
lidar-sim ─MQTT─▶ EMQX ─▶ Mqtt.Agent ─gRPC─▶ Engine ─NATS─▶ intersyslink.workflow.ot.lidar.all.tags
                                                                  │  (일반 구독)
                                                                  ▼
                                                    nats-tag-stream :64080 ─WebSocket─▶ /ws/tags
```

Engine 메시지는 약 1초에 한 번, 태그 400~490개 · 약 300KB 다. 그 안의 `value` 는 raw_payload 를 한 번 더 감싼
JSON 문자열(`\u0022` 이스케이프)이라 여기서 풀어 객체로 넘긴다. TagId 에서 장비 · 채널 · 산출물 종류도 잘라 둔다.

## 띄우기

Aspire 를 다시 띄우면 NATS 포트와 비밀번호가 바뀐다. 대시보드(http://localhost:15147)의 nats 연결 문자열을 넘긴다.

```powershell
$env:JAVA_HOME = "$env:USERPROFILE\.jdks\corretto-21.0.12"
.\gradlew.bat bootJar
$env:NATS_URL = "nats://nats:<비밀번호>@127.0.0.1:<포트>"
java -jar build\libs\nats-tag-stream-0.1.0.jar
```

| 환경변수 | 기본값 | |
|---|---|---|
| `NATS_URL` | `nats://127.0.0.1:4222` | 호스트는 `127.0.0.1` 로 (podman 이 IPv4 에만 바인드) |
| `NATS_SUBJECT` | `intersyslink.workflow.ot.lidar.all.tags` | 와일드카드 가능 (`intersyslink.workflow.>`) |
| `SERVER_PORT` | `64080` | |

상태: http://localhost:64080/actuator/health 의 `nats` 항목(연결 상태 · 마지막 수신 시각).
지표: `/actuator/prometheus` 의 `tagstream_nats_messages_total` · `tagstream_nats_tags_total` ·
`tagstream_ws_sessions` · `tagstream_ws_frames_sent_total` · `tagstream_ws_send_failures_total`.

## 접속

```
ws://localhost:64080/ws/tags                                   전부
ws://localhost:64080/ws/tags?channel=status                    채널만 (status · actual · artifact, 쉼표로 여럿)
ws://localhost:64080/ws/tags?device=LDR-GJ-A1B2                장비 id 앞부분 — 조립 1공장 2베이 전체
ws://localhost:64080/ws/tags?channel=actual,artifact&device=LDR-GJ-A1B2-01
```

접속하면 `hello` 가 한 번 오고, 이후 NATS 메시지마다 거른 결과가 한 프레임으로 온다(비면 안 보낸다).

```jsonc
{"type":"hello","channels":["actual"],"devicePrefixes":["LDR-GJ-A1B2"]}

{"type":"tags",
 "subject":"intersyslink.workflow.ot.lidar.all.tags",
 "edgeGroupId":"edge.mqtt.p3.ot",
 "receivedAt":"2026-09-22T00:35:30.156Z",          // 이 서버가 NATS 에서 받은 시각
 "count":3,
 "tags":[{"tagId":"LDR-GJ-A1B2-01-SEGMENTED_PCD.ot_pipeline_assembly_assembly1_bay2_artifact.raw_payload",
          "device":"LDR-GJ-A1B2-01",
          "artifactType":"SEGMENTED_PCD",            // 산출물만
          "topicKey":"ot_pipeline_assembly_assembly1_bay2_artifact",
          "channel":"artifact",
          "field":"raw_payload",
          "valueKind":"String",
          "changedAt":"2026-09-22T00:35:29.698+00:00",  // = raw_payload.occurred_at. 도착 순서가 아니다
          "value":{"scan_id":"…","segment_id":"SEG-003", …}}]}
```

브라우저에서는 라이브러리 없이 된다.

```js
const ws = new WebSocket("ws://localhost:64080/ws/tags?channel=status");
ws.onmessage = e => { const m = JSON.parse(e.data); if (m.type === "tags") console.log(m.count, m.tags[0]); };
```

## 알아둘 것

- **일반 구독이라 붙기 전 메시지는 없고 재전달도 없다.** 실시간 화면용이다. 적재는 Kafka 경로를 쓴다.
- NATS 까지는 세그먼트 PCD 가 합쳐지지 않는다. 같은 tagId 가 한 프레임에 여러 번 올 수 있다(`segment_id` 가 다름).
  Kafka 경로에서 세그먼트가 줄어드는 것은 그다음 EES Kafka Provider 에서다.
- 느린 클라이언트는 한 번 보내는 데 5초를 넘기거나 밀린 양이 4MB 를 넘으면 그 세션만 끊긴다(close code `4500`).
  다른 세션과 NATS 수신에는 영향이 없다. 필터 없이 받으면 약 300KB/초다.
- NATS 가 아직 없어도 앱은 뜬다. 뒤에서 2초 간격으로 계속 붙어 보고, 상태는 health 로 본다.
