#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# 단계 부하 검증 — 사다리를 올리며 INSERT / UPDATE / DELETE 를 흘리고 누락을 본다.
#
#   ./scripts/staged-load.sh                    # 기본 사다리
#   ./scripts/staged-load.sh 1000 10000         # 단계를 직접 지정
#
# 한 단계는 셋으로 이뤄진다. 각각 끝날 때마다 원본과 수신의 행 수가 같아질 때까지
# 기다렸다가 걸린 시간을 잰다 — 기다림 없이 다음 단계로 가면 앞 단계의 밀림이
# 뒤 단계의 측정에 섞여 무엇이 느린지 알 수 없게 된다.
#
#   INSERT cnt건  →  UPDATE cnt/10건  →  DELETE cnt/20건
#
# UPDATE·DELETE 를 INSERT 와 같은 건수로 돌리지 않는 이유는 두 가지다.
#   (1) DELETE 를 같은 수로 돌리면 방금 넣은 것을 도로 지워 사다리가 오르지 않는다.
#   (2) 운영 부하가 대개 읽기·삽입 위주라 1:0.1:0.05 쪽이 실제에 가깝다.
#
# 누락 판정은 "행 수가 같아지는가"로 한다. 값까지 보려면 끝에 한 번
# scripts/reconcile.sql 로 체크섬을 맞춘다 — 매 단계 돌리기에는 전체 스캔이라 비싸다.
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

SRC=emb-cdc-source-pg
DST=emb-cdc-target-pg

# 사다리. 인자를 주면 그것을 쓴다.
if [ $# -gt 0 ]; then
    STAGES=("$@")
else
    STAGES=(1000 10000 100000 200000 300000 400000 500000 1000000)
fi

# 한 단계를 기다리는 최대 시간(초). 100만 건이 10분 남짓이라 넉넉히 잡는다.
WAIT_MAX="${WAIT_MAX:-1800}"

psql_src() { $engine exec -i "$SRC" psql -U postgres -d sourcedb -v ON_ERROR_STOP=1 "$@"; }
q_src() { $engine exec -i "$SRC" psql -U postgres -d sourcedb -tAc "$1"; }
q_dst() { $engine exec -i "$DST" psql -U postgres -d targetdb -tAc "$1"; }

# 원본 세 표의 합계. car 는 하드 삭제라 그대로 세고, 나머지 둘은 수신에서
# 소프트 삭제 행을 빼야 같은 기준이 된다.
src_total() {
    q_src "SELECT (SELECT count(*) FROM car) + (SELECT count(*) FROM computer)
                + (SELECT count(*) FROM member)"
}
dst_total() {
    q_dst "SELECT (SELECT count(*) FROM car) + (SELECT count(*) FROM computer WHERE deleted = false)
                + (SELECT count(*) FROM member WHERE deleted = false)"
}

# 원본과 수신이 같아질 때까지 기다린다. 반환은 걸린 초.
# 마지막 값이 30초 동안 움직이지 않으면 멈춘 것으로 보고 끊는다 — 무한정 기다리면
# 파이프라인이 죽은 것과 느린 것을 구분할 수 없다.
wait_sync() {
    local want="$1" t0 now got last stall
    t0=$(date +%s); last=-1; stall=0
    while :; do
        got=$(dst_total)
        [ "$got" = "$want" ] && { echo $(( $(date +%s) - t0 )); return 0; }
        if [ "$got" = "$last" ]; then
            stall=$(( stall + 2 ))
            [ "$stall" -ge 30 ] && { echo "STALL:$got"; return 1; }
        else
            stall=0; last=$got
        fi
        now=$(( $(date +%s) - t0 ))
        [ "$now" -ge "$WAIT_MAX" ] && { echo "TIMEOUT:$got"; return 1; }
        sleep 2
    done
}

run_phase() {  # $1=라벨 $2=스크립트 $3=건수
    local label="$1" sql="$2" cnt="$3" before after want elapsed
    before=$(src_total)
    local s0=$(date +%s)
    psql_src -v cnt="$cnt" -f - < "$root/scripts/$sql" >/dev/null 2>&1
    local commit_s=$(( $(date +%s) - s0 ))
    want=$(src_total)
    elapsed=$(wait_sync "$want")
    if [[ "$elapsed" == STALL:* || "$elapsed" == TIMEOUT:* ]]; then
        printf '  %-8s %9s건  커밋 %4ss  반영 %-14s 원본 %s / 수신 %s  << 누락\n' \
            "$label" "$(printf "%'d" "$cnt")" "$commit_s" "$elapsed" "$want" "${elapsed#*:}"
        return 1
    fi
    printf '  %-8s %9s건  커밋 %4ss  반영 %4ss   원본=수신 %s\n' \
        "$label" "$(printf "%'d" "$cnt")" "$commit_s" "$elapsed" "$(printf "%'d" "$want")"
    return 0
}

echo "── 단계 부하 검증 ───────────────────────────────────────────────"
echo "  사다리: ${STAGES[*]}"
echo "  시작 시각: $(date '+%F %T')   시작 행 수: 원본 $(src_total) / 수신 $(dst_total)"
echo

fail=0
for cnt in "${STAGES[@]}"; do
    echo "▸ 단계 $(printf "%'d" "$cnt")"
    run_phase "INSERT" seed-bulk.sql   "$cnt"            || fail=1
    run_phase "UPDATE" update-bulk.sql $(( cnt / 10 ))   || fail=1
    run_phase "DELETE" delete-bulk.sql $(( cnt / 20 ))   || fail=1
    echo
    [ "$fail" = 1 ] && { echo "누락이 확인돼 중단한다."; break; }
done

echo "── 최종 대조 ────────────────────────────────────────────────────"
s=$(src_total); d=$(dst_total)
echo "  원본 $(printf "%'d" "$s") / 수신 $(printf "%'d" "$d")   차이 $(( s - d ))"
echo "  표별:"
q_src "SELECT 'car='||count(*) FROM car UNION ALL SELECT 'computer='||count(*) FROM computer
       UNION ALL SELECT 'member='||count(*) FROM member" | tr '\n' ' '
echo "  (원본)"
q_dst "SELECT 'car='||count(*) FROM car UNION ALL SELECT 'computer='||count(*) FROM computer WHERE deleted=false
       UNION ALL SELECT 'member='||count(*) FROM member WHERE deleted=false" | tr '\n' ' '
echo "  (수신)"
echo "  DLQ PENDING: $(q_dst "SELECT count(*) FROM cdc_dead_letter WHERE status='PENDING'")"
echo "  종료 시각: $(date '+%F %T')"
[ "$s" = "$d" ] && echo "  판정: 누락 없음" || echo "  판정: 차이 $(( s - d ))건 — DLQ 와 지연을 확인할 것"
exit "$fail"
