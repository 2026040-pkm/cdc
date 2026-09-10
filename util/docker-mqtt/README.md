# MQTT Broker Lab

MQTT Agent(mqttProtocol agent) 검증용 로컬 브로커 스택.
Mosquitto / EMQX / NATS Server 를 **동시에** 띄워서, 같은 토픽·같은 페이로드가
브로커를 바꿔도 동일하게 동작하는지 확인하는 것이 목적이다.

문서: `D:\docs\hanwha_init\도메인\06.interSysLink\개발\MQTT Broker 환경 셋팅.md`

## 실행

```powershell
# 이 프로젝트는 podman 을 쓴다 (Aspire 설정과 동일)
podman compose up -d                      # 브로커 3종 + mqtt-cli (mosquitto 클라이언트)
podman compose --profile tools up -d      # nats-box (NATS 네이티브 CLI)
podman compose --profile sim   up -d      # 장비 heartbeat 시뮬레이터

podman compose down -v                    # 볼륨까지 정리
```

`docker` 를 쓴다면 `podman` 을 `docker` 로 바꾸면 그대로 동작한다.

## 포트

| 브로커 | MQTT | 그 외 |
|---|---|---|
| Mosquitto | **1883** | 9001 (WebSocket) |
| EMQX | **1884** | 8083 (WS), 18083 (Dashboard, admin/public) |
| NATS | **1885** | 14222 (NATS 네이티브), 18222 (모니터링) |

- 컨테이너 **내부**에서는 세 브로커 모두 `1883` 이다 → `mosquitto_pub -h emqx -p 1883 ...`
- `mqtt-cli` 컨테이너는 `compose up -d` 에 포함된다(프로파일 없음). 여기서 붙을 때는
  `host.containers.internal:1884` 같은 호스트 경유가 아니라 `emqx:1883` 을 쓴다.
- NATS 네이티브 포트를 14222/18222 로 올린 이유: interSysLink(Aspire) 로컬 스택이
  이미 `4222`/`4223` 을 점유하고 있어서 그대로 두면 기동이 실패한다.

## 빠른 확인

```powershell
# 발행
podman exec mqtt-cli mosquitto_pub -h mosquitto -p 1883 `
  -t ot/device/fabrication/lidar/status -m '{\"id\":\"lidar-01\"}' -q 1 -r

# 구독 (와일드카드)
podman exec mqtt-cli mosquitto_sub -h mosquitto -p 1883 -t "ot/device/#" -v

# NATS 로 들어간 메시지가 어떤 subject 가 되는지
podman exec mqtt-nats-box nats sub -s nats://nats:4222 ">"
```

`no container with name or ID "mqtt-cli" found` 가 나면 `mqtt-cli` 가 안 떠 있는 것이다.
`podman compose up -d` 로 다시 올리고 `podman ps` 로 확인한다.

## 시뮬레이터 옵션

`publisher` 서비스의 환경변수로 검증 시나리오를 바꾼다 (`docker-compose.yml` 참고).

| 변수 | 값 | 용도 |
|---|---|---|
| `BROKER_HOST` | `mosquitto` \| `emqx` \| `nats` | 대상 브로커 전환 |
| `TOPIC_SEPARATOR` | `slash` \| `dot` | NATS subject 변환 차이 실측 |
| `PAYLOAD_SCHEMA` | `generic` \| `device` | 범용 형식 / 문서 08 5-2 형식 |
| `QOS` | `0` \| `1` \| `2` | QoS 별 동작 확인 |
| `RETAIN` / `CLEAN_SESSION` | `true` \| `false` | 재접속 시 상태 공백 확인 |

## 구성 파일

```
docker-compose.yml
mosquitto/config/mosquitto.conf   # 2.0 은 listener/allow_anonymous 명시 필수
nats/nats.conf                    # jetstream + mqtt 블록 (둘 다 있어야 MQTT 동작)
publisher/publisher.py            # LIDAR heartbeat 시뮬레이터
```
