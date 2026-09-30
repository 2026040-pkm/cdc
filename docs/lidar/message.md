# LiDAR MQTT 토픽 — 실제 생성 · 개수 산정

생성 스크립트로 두 설계안의 토픽을 실제 문자열로 전개해 개수를 확정한다. 추측이 아니라 아래 가정으로 실제로 문자열을 만들어 센 값이다.

## 전제 (가정치 — 확정 아님)

- 장비 수: LiDAR 조립 210대 + 선행의장 140대 = 350대 (`util/mqtt-lidar-sim/README.md` 계약값)
- shop/bay 레이아웃: `assembly1` × bay1~7, `outfitting1` × bay1~7 — **필드 인터페이스 정의서 §9.1이 "현장 레이아웃 미확정"이라고 명시한 값**이다. cdc PoC가 부하 테스트용으로 잡은 가정치를 그대로 썼다.
- bay당 대수: 조립 210÷7=30대/bay, 의장 140÷7=20대/bay (균등 분배 가정 — 실제는 bay마다 다를 수 있음)
- device_id 명명: `LDR-GJ-{A1|O1}B{bay}-{순번 2자리}` (mqtt-lidar-sim·lidar-compare 실측 예시와 동일 규칙)
- **LiDAR 외 역할(PAN_TILT·EDGE_PC·INFERENCE_WS·VISION_OCR)은 수량이 정해진 곳이 없어 아래 집계에서 뺐다.** 있으면 Design A 쪽 상태 토픽 수에 그대로 더해진다 — Design B는 역할이 몇 개든 토픽 수가 안 변한다(§비교 참고).

## Design A — 필드 인터페이스 정의서 v0.3 문면 그대로

토픽 패턴: `ot/device/{zone}/{device_role}/{device_id}/status` · `ot/sensor/{zone}/{shop}/{bay}/actual` · `ot/pipeline/{zone}/{shop}/{bay}/artifact`

| 채널 | 계산식 | 개수 |
|---|---|---|
| 상태 | 장비 대수만큼(LiDAR만) — 350 | **350** |
| 실적 | zone×shop×bay = (1×7) + (1×7) | **14** |
| 산출물 | zone×shop×bay = (1×7) + (1×7) | **14** |
| **합계** | | **378** |

### 상태 — 350개 (일부 예시, 전체는 부록 A)

```
ot/device/assembly/lidar/LDR-GJ-A1B1-01/status
ot/device/assembly/lidar/LDR-GJ-A1B1-02/status
ot/device/assembly/lidar/LDR-GJ-A1B1-03/status
...
ot/device/assembly/lidar/LDR-GJ-A1B1-28/status
ot/device/assembly/lidar/LDR-GJ-A1B1-29/status
ot/device/assembly/lidar/LDR-GJ-A1B1-30/status
ot/device/assembly/lidar/LDR-GJ-A1B2-01/status
ot/device/assembly/lidar/LDR-GJ-A1B2-02/status
ot/device/assembly/lidar/LDR-GJ-A1B2-03/status
...
ot/device/outfitting/lidar/LDR-GJ-O1B7-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-20/status
```

### 실적 — 14개 (전체)

```
ot/sensor/assembly/assembly1/bay1/actual
ot/sensor/assembly/assembly1/bay2/actual
ot/sensor/assembly/assembly1/bay3/actual
ot/sensor/assembly/assembly1/bay4/actual
ot/sensor/assembly/assembly1/bay5/actual
ot/sensor/assembly/assembly1/bay6/actual
ot/sensor/assembly/assembly1/bay7/actual
ot/sensor/outfitting/outfitting1/bay1/actual
ot/sensor/outfitting/outfitting1/bay2/actual
ot/sensor/outfitting/outfitting1/bay3/actual
ot/sensor/outfitting/outfitting1/bay4/actual
ot/sensor/outfitting/outfitting1/bay5/actual
ot/sensor/outfitting/outfitting1/bay6/actual
ot/sensor/outfitting/outfitting1/bay7/actual
```

### 산출물 — 14개 (전체)

```
ot/pipeline/assembly/assembly1/bay1/artifact
ot/pipeline/assembly/assembly1/bay2/artifact
ot/pipeline/assembly/assembly1/bay3/artifact
ot/pipeline/assembly/assembly1/bay4/artifact
ot/pipeline/assembly/assembly1/bay5/artifact
ot/pipeline/assembly/assembly1/bay6/artifact
ot/pipeline/assembly/assembly1/bay7/artifact
ot/pipeline/outfitting/outfitting1/bay1/artifact
ot/pipeline/outfitting/outfitting1/bay2/artifact
ot/pipeline/outfitting/outfitting1/bay3/artifact
ot/pipeline/outfitting/outfitting1/bay4/artifact
ot/pipeline/outfitting/outfitting1/bay5/artifact
ot/pipeline/outfitting/outfitting1/bay6/artifact
ot/pipeline/outfitting/outfitting1/bay7/artifact
```

## Design B — cdc 검증 방식 (권장, device_role·device_id·shop·bay를 등록 행/payload로 이동)

토픽 패턴: `ot/device/{zone}/status` · `ot/sensor/{zone}/actual` · `ot/pipeline/{zone}/artifact`

장비·역할·shop·bay 구분은 Agent의 등록 CSV(topic+deviceId 행)와 payload 필드로 옮기고, 토픽은 브로커가 실제로 물리 분리되는 단위(zone)까지만 둔다 — 근거는 `MqttPayloadParser`가 `{id}+토픽`으로 TagId를 재구성해 필터링한다는 것, 그리고 `ConfigDtos.ReadTopics`가 **동일 토픽 문자열을 자동으로 dedup**한다는 것(둘 다 소스 확인).

| 채널 | 개수 |
|---|---|
| 상태 | **2** |
| 실적 | **2** |
| 산출물 | **2** |
| **합계** | **6** |

### 전체 목록 (6개)

```
ot/device/assembly/status
ot/device/outfitting/status
ot/sensor/assembly/actual
ot/sensor/outfitting/actual
ot/pipeline/assembly/artifact
ot/pipeline/outfitting/artifact
```

## 비교

| | Design A (스펙 문면) | Design B (권장) | 비율 |
|---|---|---|---|
| 토픽 합계 | 378 | 6 | 63배 |
| 장비 1대 늘 때 | +1 토픽 | +0 (등록 행만 +1) | — |
| 역할 1종 늘 때 | 그 역할 장비 수만큼 +토픽 | +0 | — |
| Agent 기동 시 SUBSCRIBE 횟수 | 378회(순차 대기) | 6회 | — |

Design A는 350대라는 지금 규모 기준 수치이고, 다른 역할(PAN_TILT 등)이 추가되거나 필리조선소로 확장되면(§4.1) 장비 수에 비례해 그대로 늘어난다. Design B는 zone 수(현재 2개)에만 묶여 있어 장비가 몇 대가 되든 6개로 고정된다.

## 토픽을 세분화할지 최소화할지 — 관리 방식 분석

숫자 차이(378 vs 6)만 보면 무조건 Design B가 맞아 보이지만, 세분화가 실제로 값어치를 하는 경우도 있다. 여기서는 두 관리 방식을 지금 이 시스템의 실제 조건(Agent 소스로 확인한 것)에 비춰 따진다.

### 세분화(Design A)가 정당화되는 조건 — 지금 몇 개나 해당하는지

| 조건 | 세분화가 필요한 이유 | 이 프로젝트에 해당하는가 |
|---|---|---|
| 토픽 단위 접근 제어(ACL) 필요 | 특정 벤더·파트너가 자기 구역에만 publish하도록 브로커가 강제해야 함 | 문서 어디에도 이런 요구 없음 — publish 주체는 zone당 하나(AI Inference Service 또는 LiDAR Edge 서비스)뿐 |
| Agent 외 제3의 컨슈머가 브로커에 직접 붙어 부분 구독 | 다른 시스템이 EMQX에서 특정 장비·구역만 골라 구독 | §3 흐름도상 EMQX 구독자는 MQTT Agent 하나뿐 |
| 토픽별로 QoS·retain 등 MQTT 속성을 다르게 줘야 함 | 일부 장비만 다른 전달 보장이 필요 | 전 채널 QoS 1로 동일, 이런 요구 없음 |
| ISL 벤더 도구가 "토픽 1개 = 태그 1개"만 지원 | 공유 토픽에 여러 태그를 못 묶음 | **아니다** — 등록 CSV가 `topic`·`deviceId`를 별도 컬럼으로 두고, `MqttPayloadParser`가 `id`+토픽으로 TagId를 재구성해 필터링한다(코드 확인). 공유 토픽 자체가 이미 지원되는 경로다 |

네 조건 다 지금은 해당 사항이 없다 — 즉 **세분화를 정당화하는 근거가 하나도 없는 상태**에서 비용(378개, 순차 SUBSCRIBE 378회, 관리 대상 증가)만 지고 있는 모양이다.

### 최소화(Design B)가 지는 위험 — 없어지는 게 아니라 옮겨가는 것

| 잃는 것 | 옮겨가는 곳 |
|---|---|
| 장비 하나만 mosquitto_sub로 격리해서 보는 디버깅 편의 | 클라이언트에서 payload의 `id` 필드로 필터(예: `mosquitto_sub -t ot/device/assembly/status | grep LDR-GJ-A1B3-01`) — 되긴 하지만 브로커가 걸러주지 않는다 |
| 토픽 문자열만 보고 장비를 알 수 있는 자기서술성 | payload를 열어야 안다 — Engine 태그 카탈로그에서 장비 하나를 찾을 때 검색 기준이 TagId 대신 device_id 필드가 된다 |
| 향후 ACL 요구가 생겼을 때 바로 대응 가능한 구조 | 그 요구가 실제로 생기면 그때 토픽을 다시 쪼개야 한다(재등록·TagId 재발급 비용 발생) |
| "device_id-in-topic이 ISL 벤더 프로덕션에서도 보장되는가"(§9.2 미확인) | 지금 근거는 `intersyslink-v4`의 `mqtt-agent` **브랜치·미커밋** 코드 하나뿐이다 — 벤더가 공식 확인 전까지는 이 코드가 그대로 출시된다는 보장이 없다 |

마지막 항목이 제일 중요하다 — Design B 전체가 딛고 있는 근거(공유 토픽 + deviceId 필터링이 된다)가 **아직 벤더 확인을 못 받은 브랜치 코드 하나**라는 뜻이다. 이 검증이 실제 출시 버전과 다르면 Design B는 통째로 재작업이 필요하다.

### 결론

지금 조건(단일 컨슈머, ACL 요구 없음, 등록 CSV 구조가 공유 토픽을 이미 지원)에서는 **최소화(Design B)가 맞다** — 세분화의 이점을 실제로 쓰는 곳이 없는데 비용(378배 토픽 수, 기동 시간, 관리 대상)만 지불하고 있기 때문이다.

**재검토 조건(아래 중 하나라도 생기면 다시 세분화 검토):**
1. Agent 외의 시스템이 EMQX를 직접 구독해야 하는 요구가 생긴다
2. 특정 벤더·구역에 브로커 레벨 publish 권한 분리가 필요해진다
3. ISL 벤더가 "공유 토픽 + deviceId 필터" 방식을 프로덕션에서 지원 안 한다고 공식 확인한다(§9.2)
4. `mqtt-agent` 브랜치가 머지되지 않거나, 머지된 버전에서 이 필터링 동작이 바뀐다

## 실적(actual) 채널 — stage 처리 현황

토픽 개수 산정과는 별개로, 실적 채널 payload에서 `stage`가 빠진(v0.3) 뒤 실제 소비 화면 쪽에서 새로 확인된 사항이다.

### 확정된 것
- 토픽·payload 어디에도 `stage`가 없다 — 필드 인터페이스 정의서 v0.3 §4.3·§6.3. LiDAR는 형상 대조로 진척률만 낼 뿐 공정 단계(ARRANGEMENT/PROCESSING/INSPECTION/OUTBOUND)를 판별하지 못한다는 게 이유
- 이 변경은 위 Design A/B 토픽 개수(실적 14개, Design B 2개)에는 영향이 없다 — 개수는 zone×shop×bay로만 정해지고 stage는 애초에 곱하지 않았다(§토픽을 세분화할지 최소화할지)

### 새로 발견한 것 — 이미 구현된 화면이 다른 경로로 "공정 단계"를 쓰고 있었다
`PRD_조립공장뷰_LiDAR_2.5D_개선.md` FR-5(2026-08-26 구현 완료)의 베이 강조 규칙이 이미 `bayStatusSummary.bayStage`라는 값을 쓴다. 이 값은 "송선 현공정 코드" — **레거시 시스템 개념**을 기준으로 한다. 즉 지금 화면에는:

- LiDAR MQTT 실적 메시지의 `stage` (v0.3에서 뺀 것, 원래도 형상 대조만으로는 못 만드는 값이었다)
- 이미 동작 중인 `bayStage` (레거시 송선코드 기준)

라는 **서로 다른 두 "공정 단계" 개념**이 있다. 필드 인터페이스 정의서 §9.4는 "판별 서비스가 사후에 stage를 매긴다"고만 적어뒀는데, 그 산출물이 이 레거시 `bayStage`와 같은 것인지 별개로 새로 만들 것인지가 정의돼 있지 않다.

### 확인 안 하면 생기는 문제
판별 서비스가 나중에 stage를 산출해도, 화면(FR-5)이 그걸 쓸지 계속 레거시 `bayStage`를 쓸지 결정이 안 돼 있으면 같은 화면에 서로 다른 기준의 "공정 단계"가 섞여 보일 수 있다.

### 영향 문서
- `필드인터페이스정의서.md` §9.4 — "조립·선행의장 공정 단계(stage) 판별 방법 미해결" 항목에 레거시 `bayStage` 존재를 반영해야 함(아직 미반영)
- `PRD_조립공장뷰_LiDAR_2.5D_개선.md` §15 오픈 이슈 10번 — 반영 완료(2026-09-22)

## 실제 구현 확인 — `util/mqtt-lidar-sim/lidar-sim.cs`

토픽 개수는 Design B 그대로 나왔지만, **태그 그레인은 예상과 다르게 갔다.** 소스를 직접 읽어 확인한 결과다.

| 채널 | 토픽(zone당 1개, 6개 확정) | payload `id` | 결과 태그 수 |
|---|---|---|---|
| 상태 | `ot/device/{zone}/status` | `device.Id` | 350 (조립 210 + 의장 140) |
| 실적 | `ot/sensor/{zone}/actual` | **`device.Id`** — shop/bay 합성값 아님 | **350** |
| 산출물 | `ot/pipeline/{zone}/artifact` | `{device.Id}-{artifactType}` | 350 × 3종 = 1,050 |

**토픽 수(6개)는 맞았지만, 실적·산출물의 등록 수는 위 Design A/B 비교(14개)와 다르다.** 이전 절들이 "실적·산출물 태그는 shop×bay 조합만큼(14개)"이라고 쓴 건 필드 인터페이스 정의서 §5.1의 등록 수 공식을 그대로 따른 가정이었는데, 실제 구현은 **shop/bay 합성 id를 새로 만들지 않고 이미 있는 device_id를 그대로 재사용**했다. 그 결과:

- 실적 채널은 스펙이 의도한 "shop/bay 채널 태그"(§5.1 "shop×bay 조합 × 1")가 아니라 **여전히 장비별 태그**다. 등록도 350개, cdc의 원래 실적 채널 그레인(장비별)을 그대로 유지한 셈이다.
- 산출물도 원래 cdc 그레인(장비+종류별, zone당 630~1,050)을 그대로 유지했다 — "bay당 1개"로 합치는 방향은 채택되지 않았다.
- 실적 payload는 `device_ids` 중첩 객체 대신 `pan_tilt`·`edge_pc`·`inference_ws`·`vision_ocr`를 최상위에 바로 평탄화해서 쓴다.
- 산출물 payload에는 `idempotency_key`가 없다(상태·실적에는 있음) — 필드 인터페이스 정의서 §6.4 스키마와 다른 점.
- `id` 필드가 없어서 태그가 안 만들어지는 문제(§07②로 남겨뒀던 것)는 **이미 해결돼 있었다** — 새 필드를 추가한 게 아니라 원래 있던 device_id를 그대로 썼을 뿐이다.

## 남는 미확정 (이 집계가 못 채우는 것)

- shop/bay 레이아웃 자체가 미확정(§9.1) — 실제 현장 값이 나오면 실적·산출물 토픽 수(Design A 기준 14개)가 바뀐다
- PAN_TILT·EDGE_PC·INFERENCE_WS·VISION_OCR 수량 미확정 — Design A 상태 토픽 수(350)의 하한선일 뿐
- 실적·산출물을 스펙 의도대로 shop/bay 단위 채널 태그로 합칠지, 지금처럼 장비별 그레인을 유지할지는 여전히 결정 사항 — 지금 구현은 후자를 택했다

## 부록 A — Design A 상태 토픽 전체 350개

```
ot/device/assembly/lidar/LDR-GJ-A1B1-01/status
ot/device/assembly/lidar/LDR-GJ-A1B1-02/status
ot/device/assembly/lidar/LDR-GJ-A1B1-03/status
ot/device/assembly/lidar/LDR-GJ-A1B1-04/status
ot/device/assembly/lidar/LDR-GJ-A1B1-05/status
ot/device/assembly/lidar/LDR-GJ-A1B1-06/status
ot/device/assembly/lidar/LDR-GJ-A1B1-07/status
ot/device/assembly/lidar/LDR-GJ-A1B1-08/status
ot/device/assembly/lidar/LDR-GJ-A1B1-09/status
ot/device/assembly/lidar/LDR-GJ-A1B1-10/status
ot/device/assembly/lidar/LDR-GJ-A1B1-11/status
ot/device/assembly/lidar/LDR-GJ-A1B1-12/status
ot/device/assembly/lidar/LDR-GJ-A1B1-13/status
ot/device/assembly/lidar/LDR-GJ-A1B1-14/status
ot/device/assembly/lidar/LDR-GJ-A1B1-15/status
ot/device/assembly/lidar/LDR-GJ-A1B1-16/status
ot/device/assembly/lidar/LDR-GJ-A1B1-17/status
ot/device/assembly/lidar/LDR-GJ-A1B1-18/status
ot/device/assembly/lidar/LDR-GJ-A1B1-19/status
ot/device/assembly/lidar/LDR-GJ-A1B1-20/status
ot/device/assembly/lidar/LDR-GJ-A1B1-21/status
ot/device/assembly/lidar/LDR-GJ-A1B1-22/status
ot/device/assembly/lidar/LDR-GJ-A1B1-23/status
ot/device/assembly/lidar/LDR-GJ-A1B1-24/status
ot/device/assembly/lidar/LDR-GJ-A1B1-25/status
ot/device/assembly/lidar/LDR-GJ-A1B1-26/status
ot/device/assembly/lidar/LDR-GJ-A1B1-27/status
ot/device/assembly/lidar/LDR-GJ-A1B1-28/status
ot/device/assembly/lidar/LDR-GJ-A1B1-29/status
ot/device/assembly/lidar/LDR-GJ-A1B1-30/status
ot/device/assembly/lidar/LDR-GJ-A1B2-01/status
ot/device/assembly/lidar/LDR-GJ-A1B2-02/status
ot/device/assembly/lidar/LDR-GJ-A1B2-03/status
ot/device/assembly/lidar/LDR-GJ-A1B2-04/status
ot/device/assembly/lidar/LDR-GJ-A1B2-05/status
ot/device/assembly/lidar/LDR-GJ-A1B2-06/status
ot/device/assembly/lidar/LDR-GJ-A1B2-07/status
ot/device/assembly/lidar/LDR-GJ-A1B2-08/status
ot/device/assembly/lidar/LDR-GJ-A1B2-09/status
ot/device/assembly/lidar/LDR-GJ-A1B2-10/status
ot/device/assembly/lidar/LDR-GJ-A1B2-11/status
ot/device/assembly/lidar/LDR-GJ-A1B2-12/status
ot/device/assembly/lidar/LDR-GJ-A1B2-13/status
ot/device/assembly/lidar/LDR-GJ-A1B2-14/status
ot/device/assembly/lidar/LDR-GJ-A1B2-15/status
ot/device/assembly/lidar/LDR-GJ-A1B2-16/status
ot/device/assembly/lidar/LDR-GJ-A1B2-17/status
ot/device/assembly/lidar/LDR-GJ-A1B2-18/status
ot/device/assembly/lidar/LDR-GJ-A1B2-19/status
ot/device/assembly/lidar/LDR-GJ-A1B2-20/status
ot/device/assembly/lidar/LDR-GJ-A1B2-21/status
ot/device/assembly/lidar/LDR-GJ-A1B2-22/status
ot/device/assembly/lidar/LDR-GJ-A1B2-23/status
ot/device/assembly/lidar/LDR-GJ-A1B2-24/status
ot/device/assembly/lidar/LDR-GJ-A1B2-25/status
ot/device/assembly/lidar/LDR-GJ-A1B2-26/status
ot/device/assembly/lidar/LDR-GJ-A1B2-27/status
ot/device/assembly/lidar/LDR-GJ-A1B2-28/status
ot/device/assembly/lidar/LDR-GJ-A1B2-29/status
ot/device/assembly/lidar/LDR-GJ-A1B2-30/status
ot/device/assembly/lidar/LDR-GJ-A1B3-01/status
ot/device/assembly/lidar/LDR-GJ-A1B3-02/status
ot/device/assembly/lidar/LDR-GJ-A1B3-03/status
ot/device/assembly/lidar/LDR-GJ-A1B3-04/status
ot/device/assembly/lidar/LDR-GJ-A1B3-05/status
ot/device/assembly/lidar/LDR-GJ-A1B3-06/status
ot/device/assembly/lidar/LDR-GJ-A1B3-07/status
ot/device/assembly/lidar/LDR-GJ-A1B3-08/status
ot/device/assembly/lidar/LDR-GJ-A1B3-09/status
ot/device/assembly/lidar/LDR-GJ-A1B3-10/status
ot/device/assembly/lidar/LDR-GJ-A1B3-11/status
ot/device/assembly/lidar/LDR-GJ-A1B3-12/status
ot/device/assembly/lidar/LDR-GJ-A1B3-13/status
ot/device/assembly/lidar/LDR-GJ-A1B3-14/status
ot/device/assembly/lidar/LDR-GJ-A1B3-15/status
ot/device/assembly/lidar/LDR-GJ-A1B3-16/status
ot/device/assembly/lidar/LDR-GJ-A1B3-17/status
ot/device/assembly/lidar/LDR-GJ-A1B3-18/status
ot/device/assembly/lidar/LDR-GJ-A1B3-19/status
ot/device/assembly/lidar/LDR-GJ-A1B3-20/status
ot/device/assembly/lidar/LDR-GJ-A1B3-21/status
ot/device/assembly/lidar/LDR-GJ-A1B3-22/status
ot/device/assembly/lidar/LDR-GJ-A1B3-23/status
ot/device/assembly/lidar/LDR-GJ-A1B3-24/status
ot/device/assembly/lidar/LDR-GJ-A1B3-25/status
ot/device/assembly/lidar/LDR-GJ-A1B3-26/status
ot/device/assembly/lidar/LDR-GJ-A1B3-27/status
ot/device/assembly/lidar/LDR-GJ-A1B3-28/status
ot/device/assembly/lidar/LDR-GJ-A1B3-29/status
ot/device/assembly/lidar/LDR-GJ-A1B3-30/status
ot/device/assembly/lidar/LDR-GJ-A1B4-01/status
ot/device/assembly/lidar/LDR-GJ-A1B4-02/status
ot/device/assembly/lidar/LDR-GJ-A1B4-03/status
ot/device/assembly/lidar/LDR-GJ-A1B4-04/status
ot/device/assembly/lidar/LDR-GJ-A1B4-05/status
ot/device/assembly/lidar/LDR-GJ-A1B4-06/status
ot/device/assembly/lidar/LDR-GJ-A1B4-07/status
ot/device/assembly/lidar/LDR-GJ-A1B4-08/status
ot/device/assembly/lidar/LDR-GJ-A1B4-09/status
ot/device/assembly/lidar/LDR-GJ-A1B4-10/status
ot/device/assembly/lidar/LDR-GJ-A1B4-11/status
ot/device/assembly/lidar/LDR-GJ-A1B4-12/status
ot/device/assembly/lidar/LDR-GJ-A1B4-13/status
ot/device/assembly/lidar/LDR-GJ-A1B4-14/status
ot/device/assembly/lidar/LDR-GJ-A1B4-15/status
ot/device/assembly/lidar/LDR-GJ-A1B4-16/status
ot/device/assembly/lidar/LDR-GJ-A1B4-17/status
ot/device/assembly/lidar/LDR-GJ-A1B4-18/status
ot/device/assembly/lidar/LDR-GJ-A1B4-19/status
ot/device/assembly/lidar/LDR-GJ-A1B4-20/status
ot/device/assembly/lidar/LDR-GJ-A1B4-21/status
ot/device/assembly/lidar/LDR-GJ-A1B4-22/status
ot/device/assembly/lidar/LDR-GJ-A1B4-23/status
ot/device/assembly/lidar/LDR-GJ-A1B4-24/status
ot/device/assembly/lidar/LDR-GJ-A1B4-25/status
ot/device/assembly/lidar/LDR-GJ-A1B4-26/status
ot/device/assembly/lidar/LDR-GJ-A1B4-27/status
ot/device/assembly/lidar/LDR-GJ-A1B4-28/status
ot/device/assembly/lidar/LDR-GJ-A1B4-29/status
ot/device/assembly/lidar/LDR-GJ-A1B4-30/status
ot/device/assembly/lidar/LDR-GJ-A1B5-01/status
ot/device/assembly/lidar/LDR-GJ-A1B5-02/status
ot/device/assembly/lidar/LDR-GJ-A1B5-03/status
ot/device/assembly/lidar/LDR-GJ-A1B5-04/status
ot/device/assembly/lidar/LDR-GJ-A1B5-05/status
ot/device/assembly/lidar/LDR-GJ-A1B5-06/status
ot/device/assembly/lidar/LDR-GJ-A1B5-07/status
ot/device/assembly/lidar/LDR-GJ-A1B5-08/status
ot/device/assembly/lidar/LDR-GJ-A1B5-09/status
ot/device/assembly/lidar/LDR-GJ-A1B5-10/status
ot/device/assembly/lidar/LDR-GJ-A1B5-11/status
ot/device/assembly/lidar/LDR-GJ-A1B5-12/status
ot/device/assembly/lidar/LDR-GJ-A1B5-13/status
ot/device/assembly/lidar/LDR-GJ-A1B5-14/status
ot/device/assembly/lidar/LDR-GJ-A1B5-15/status
ot/device/assembly/lidar/LDR-GJ-A1B5-16/status
ot/device/assembly/lidar/LDR-GJ-A1B5-17/status
ot/device/assembly/lidar/LDR-GJ-A1B5-18/status
ot/device/assembly/lidar/LDR-GJ-A1B5-19/status
ot/device/assembly/lidar/LDR-GJ-A1B5-20/status
ot/device/assembly/lidar/LDR-GJ-A1B5-21/status
ot/device/assembly/lidar/LDR-GJ-A1B5-22/status
ot/device/assembly/lidar/LDR-GJ-A1B5-23/status
ot/device/assembly/lidar/LDR-GJ-A1B5-24/status
ot/device/assembly/lidar/LDR-GJ-A1B5-25/status
ot/device/assembly/lidar/LDR-GJ-A1B5-26/status
ot/device/assembly/lidar/LDR-GJ-A1B5-27/status
ot/device/assembly/lidar/LDR-GJ-A1B5-28/status
ot/device/assembly/lidar/LDR-GJ-A1B5-29/status
ot/device/assembly/lidar/LDR-GJ-A1B5-30/status
ot/device/assembly/lidar/LDR-GJ-A1B6-01/status
ot/device/assembly/lidar/LDR-GJ-A1B6-02/status
ot/device/assembly/lidar/LDR-GJ-A1B6-03/status
ot/device/assembly/lidar/LDR-GJ-A1B6-04/status
ot/device/assembly/lidar/LDR-GJ-A1B6-05/status
ot/device/assembly/lidar/LDR-GJ-A1B6-06/status
ot/device/assembly/lidar/LDR-GJ-A1B6-07/status
ot/device/assembly/lidar/LDR-GJ-A1B6-08/status
ot/device/assembly/lidar/LDR-GJ-A1B6-09/status
ot/device/assembly/lidar/LDR-GJ-A1B6-10/status
ot/device/assembly/lidar/LDR-GJ-A1B6-11/status
ot/device/assembly/lidar/LDR-GJ-A1B6-12/status
ot/device/assembly/lidar/LDR-GJ-A1B6-13/status
ot/device/assembly/lidar/LDR-GJ-A1B6-14/status
ot/device/assembly/lidar/LDR-GJ-A1B6-15/status
ot/device/assembly/lidar/LDR-GJ-A1B6-16/status
ot/device/assembly/lidar/LDR-GJ-A1B6-17/status
ot/device/assembly/lidar/LDR-GJ-A1B6-18/status
ot/device/assembly/lidar/LDR-GJ-A1B6-19/status
ot/device/assembly/lidar/LDR-GJ-A1B6-20/status
ot/device/assembly/lidar/LDR-GJ-A1B6-21/status
ot/device/assembly/lidar/LDR-GJ-A1B6-22/status
ot/device/assembly/lidar/LDR-GJ-A1B6-23/status
ot/device/assembly/lidar/LDR-GJ-A1B6-24/status
ot/device/assembly/lidar/LDR-GJ-A1B6-25/status
ot/device/assembly/lidar/LDR-GJ-A1B6-26/status
ot/device/assembly/lidar/LDR-GJ-A1B6-27/status
ot/device/assembly/lidar/LDR-GJ-A1B6-28/status
ot/device/assembly/lidar/LDR-GJ-A1B6-29/status
ot/device/assembly/lidar/LDR-GJ-A1B6-30/status
ot/device/assembly/lidar/LDR-GJ-A1B7-01/status
ot/device/assembly/lidar/LDR-GJ-A1B7-02/status
ot/device/assembly/lidar/LDR-GJ-A1B7-03/status
ot/device/assembly/lidar/LDR-GJ-A1B7-04/status
ot/device/assembly/lidar/LDR-GJ-A1B7-05/status
ot/device/assembly/lidar/LDR-GJ-A1B7-06/status
ot/device/assembly/lidar/LDR-GJ-A1B7-07/status
ot/device/assembly/lidar/LDR-GJ-A1B7-08/status
ot/device/assembly/lidar/LDR-GJ-A1B7-09/status
ot/device/assembly/lidar/LDR-GJ-A1B7-10/status
ot/device/assembly/lidar/LDR-GJ-A1B7-11/status
ot/device/assembly/lidar/LDR-GJ-A1B7-12/status
ot/device/assembly/lidar/LDR-GJ-A1B7-13/status
ot/device/assembly/lidar/LDR-GJ-A1B7-14/status
ot/device/assembly/lidar/LDR-GJ-A1B7-15/status
ot/device/assembly/lidar/LDR-GJ-A1B7-16/status
ot/device/assembly/lidar/LDR-GJ-A1B7-17/status
ot/device/assembly/lidar/LDR-GJ-A1B7-18/status
ot/device/assembly/lidar/LDR-GJ-A1B7-19/status
ot/device/assembly/lidar/LDR-GJ-A1B7-20/status
ot/device/assembly/lidar/LDR-GJ-A1B7-21/status
ot/device/assembly/lidar/LDR-GJ-A1B7-22/status
ot/device/assembly/lidar/LDR-GJ-A1B7-23/status
ot/device/assembly/lidar/LDR-GJ-A1B7-24/status
ot/device/assembly/lidar/LDR-GJ-A1B7-25/status
ot/device/assembly/lidar/LDR-GJ-A1B7-26/status
ot/device/assembly/lidar/LDR-GJ-A1B7-27/status
ot/device/assembly/lidar/LDR-GJ-A1B7-28/status
ot/device/assembly/lidar/LDR-GJ-A1B7-29/status
ot/device/assembly/lidar/LDR-GJ-A1B7-30/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-01/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-02/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-03/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-04/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-05/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-06/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-07/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-08/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-09/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-10/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-11/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-12/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-13/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-14/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-15/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-16/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-17/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B1-20/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-01/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-02/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-03/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-04/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-05/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-06/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-07/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-08/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-09/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-10/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-11/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-12/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-13/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-14/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-15/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-16/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-17/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B2-20/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-01/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-02/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-03/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-04/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-05/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-06/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-07/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-08/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-09/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-10/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-11/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-12/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-13/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-14/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-15/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-16/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-17/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B3-20/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-01/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-02/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-03/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-04/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-05/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-06/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-07/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-08/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-09/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-10/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-11/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-12/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-13/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-14/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-15/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-16/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-17/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B4-20/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-01/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-02/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-03/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-04/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-05/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-06/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-07/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-08/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-09/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-10/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-11/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-12/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-13/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-14/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-15/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-16/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-17/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B5-20/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-01/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-02/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-03/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-04/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-05/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-06/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-07/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-08/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-09/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-10/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-11/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-12/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-13/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-14/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-15/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-16/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-17/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B6-20/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-01/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-02/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-03/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-04/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-05/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-06/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-07/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-08/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-09/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-10/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-11/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-12/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-13/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-14/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-15/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-16/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-17/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-18/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-19/status
ot/device/outfitting/lidar/LDR-GJ-O1B7-20/status
```
