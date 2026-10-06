## 요구사항

- 각 권역별 서비스는 hotdb의 tsdb에 저장된 필드데이터를 cdc 감지하여 hotdb쪽의 rdb로 저장을 한다.
  - cdc에 대한 것의 실시간 데이터의 경우 어떤 부하가 있는지 확인 필요, - cpu, memory, 등등 부하 확인할 수 있는것
  - 각 서비스는 하나의 로컬에서가 아닌 서버 to 서버로 진행해서 테스트 해보기
- hotdb provider에 대해서 필요한 스키마 구성
  - tsdb스키마, rdb 스키마 구성 하기
- rfc agent쪽 polling, rfc provider cdc데이터 받는 것까지 해서 구현 필요
- db agent , oracle에 대한 스키마를 일정시간 polling해서 hotdb로 저장하는 것 개발 

