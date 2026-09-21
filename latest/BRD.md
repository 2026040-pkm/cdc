## cdc 및 tsdb 고려사항

- HOT DB 관련
  - 레거시 데이터 - RDB
  - 센서 데이터 - TSDB
- CDC에 대한 부분
  - db 구성 1 : 
    - source db (tsdb, rdb) -&gt; target db( tsdb, rdb)
  - db 구성 2 : 
    - source db (tsdb, rdb) -&gt; target db(rdb)
  - db 구성 3 : 
    - source db (tsdb) -&gt;  targe db(rdb) 
    - source db (rdbd) -&gt;|
  - 해당 부분에 대해서 스키마로 나누는게 좋은지 인스턴스로 분리하는게 좋은지 전략 필요
- hotdb provider 부분 mqtt agent에서 어떤 형식으로





## MQTT AGENT Mesage

```json
// ot/device/assembly/lidar/status

{
  id: {디바이스 아이디},
  raw_payload: {
    device_role: "LIDAR",
    ...
  }
}
```

