#!/bin/sh
# 레거시 빈 사본 급식기 — 원본 inshoplocal 에서 블록 하나씩 골라 inshoplocal_empty 로 옮긴다 (시험 데이터를 천천히 쌓는다).
#   원본은 읽기만 한다 (COPY (SELECT …) TO STDOUT). 이미 옮긴 블록은 건너뛴다 — 다시 띄워도 안전하다.
#   이미 있는 행은 넣지 않는다 (같은 생산오더 · 품목을 두 블록이 나눠 가질 때).
#
#   docker run -d --name hotdb-legacy-feed --restart unless-stopped \
#     -e FEED_BLOCKS="2378-154 2590-693" -e FEED_INTERVAL_S=2400 \
#     -v "$PWD/scripts/legacy-feed.sh:/feed.sh:ro" postgres:17-alpine sh /feed.sh
#
#   SRC / DST: libpq 접속 문자열. 기본은 호스트의 inshoplocal-pg(54433)
set -eu
SRC="${SRC:-host=host.docker.internal port=54433 dbname=inshoplocal user=inshoplocal password=inshoplocal}"
DST="${DST:-host=host.docker.internal port=54433 dbname=inshoplocal_empty user=inshoplocal password=inshoplocal}"
INTERVAL="${FEED_INTERVAL_S:-2400}"

copy() {   # copy <표> <WHERE 식>
  t=$1; w=$2
  { echo "CREATE TEMP TABLE f (LIKE gp.$t); COPY f FROM STDIN;"
    psql "$SRC" -v ON_ERROR_STOP=1 -Atc "COPY (SELECT * FROM gp.$t WHERE $w) TO STDOUT"
    printf '%s\n' '\.'
    echo "INSERT INTO gp.$t SELECT * FROM (SELECT * FROM f EXCEPT SELECT * FROM gp.$t) q ON CONFLICT DO NOTHING;"
  } | psql "$DST" -v ON_ERROR_STOP=1 -At | grep -E "^(COPY|INSERT)" | tr "\n" " " | sed "s/^/  $t /"; echo
}

feed() {   # feed <호선> <블록>
  h=$1; b=$2
  B="(VALUES ('$h','$b'))"
  WO="(SELECT work_ord_no FROM gp.ypwg411m WHERE (proj_no, blk_zone_no) IN $B)"
  PO="(SELECT prod_ord_no FROM gp.ypwg214m WHERE work_ord_no IN $WO)"
  DWG="(SELECT dwg_no FROM gp.ypwc310m WHERE (substr(dwg_no,1,4), substr(dwg_no,7,3)) IN $B UNION SELECT mat_desc1 FROM gp.ypwg211m WHERE prod_ord_no IN $PO AND length(mat_desc1) = 14)"
  PART="(SELECT part_no FROM gp.ydeh060m WHERE (proj_no, blk_no) IN $B)"
  PICK="(SELECT pick_ser_id FROM gp.ypwc111m WHERE dwg_no IN $DWG)"
  ASSY="(SELECT assy_no FROM gp.ydeh050m WHERE (proj_no, blk_no) IN $B)"
  RS="(SELECT rsnum FROM gp.zpptw032 WHERE (zzpjtno, zzbzs_code) IN $B)"
  # 앵커 · 근거 표가 같이 들어가게 — 부모(411 · 310M · 200) 를 마지막에 넣어 앵커가 생길 때 근거가 이미 있게 한다
  copy ypwg214m "work_ord_no IN $WO";        copy ypwg211m "prod_ord_no IN $PO";   copy ypwg212m "prod_ord_no IN $PO"
  copy ypwg413m "work_ord_no IN $WO";        copy ypwc111m "dwg_no IN $DWG";       copy ypwc140m "dwg_no IN $DWG OR pick_ser_id IN $PICK"
  copy ydeh060m "(proj_no, blk_no) IN $B";   copy ydeh090m "part_no IN $PART OR dwg_no IN $DWG"
  copy ypdg002m "part_no IN $PART";          copy ypwc821m "part_no IN $PART";     copy ypdg800m "part_no IN $PART"
  copy ypdg100m "part_no IN $PART";          copy ypwc510m "part_no IN $PART";     copy ycps301m "mat_code IN $PART"
  copy ydeh050m "(proj_no, blk_no) IN $B";   copy ydeh040m "cmpt_part_no IN $ASSY"
  copy zpptw032 "(zzpjtno, zzbzs_code) IN $B"; copy zpptw033 "rsnum IN $RS";     copy zppt031 "rsnum IN $RS"; copy zppt072 "rsnum IN $RS"
  copy ypwp720m "(proj_no, blk_no) IN $B";   copy ypwp710m "(proj_no, blk_no) IN $B"; copy zppt052 "(proj_no, block) IN $B"
  copy zcot001 "proj_noc = '$h'"
  copy ypwc310m "dwg_no IN $DWG";            copy zppt200 "(zzpjtno, zzbzs_code) IN $B"
  copy ypwg411m "work_ord_no IN $WO"
}

first=1
for hb in ${FEED_BLOCKS:-}; do
  h=${hb%-*}; b=${hb#*-}
  have=$(psql "$DST" -Atc "SELECT count(*) FROM gp.ypwg411m WHERE proj_no = '$h' AND blk_zone_no = '$b'")
  if [ "$have" != "0" ]; then echo "$(date '+%F %T') $hb 이미 있음 — 건너뜀"; continue; fi
  [ $first = 1 ] || sleep "$INTERVAL"
  first=0
  echo "$(date '+%F %T') $hb 옮기기"
  feed "$h" "$b"
  echo "$(date '+%F %T') $hb 끝"
done
echo "$(date '+%F %T') 목록 끝 — 대기"
exec sleep infinity
