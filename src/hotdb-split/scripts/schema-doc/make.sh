#!/usr/bin/env bash
# 스키마 · 데이터 흐름 문서를 기동 중인 HotDB 에서 다시 만든다.
#   bash src/hotdb/scripts/schema-doc/make.sh
# 마이그레이션(V 파일)이나 라우트를 바꾼 뒤 돌린다. 라우트 식 · 컬럼 뜻은 gen.py 의 표(SRC · ROUTE)에 있다.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p out
q() { podman exec -i hotdb-pg psql -U postgres -d hotdb -At -F'|'; }
q > out/cols.txt <<'SQL'
SELECT c.table_schema, c.table_name, c.ordinal_position, c.column_name, format_type(a.atttypid,a.atttypmod), c.is_nullable, coalesce(c.column_default,''), coalesce(c.is_identity,'')
FROM information_schema.columns c
JOIN pg_attribute a ON a.attrelid = (quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass AND a.attname=c.column_name
WHERE c.table_schema IN ('tsdb','svc','ops') AND c.table_name NOT LIKE 'flyway%' ORDER BY 1,2,3;
SQL
q > out/pk.txt <<'SQL'
SELECT n.nspname, c.relname, a.attname FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum = ANY(i.indkey) WHERE i.indisprimary AND n.nspname IN ('tsdb','svc','ops');
SQL
# 레거시 표는 별도 DB(hotdb-legacy-db). 없으면 레거시 목록만 빈다
podman exec -i hotdb-legacy-db psql -U postgres -d legacy -At -F'|' > out/legacy.txt 2>/dev/null <<'SQL' || : > out/legacy.txt
SELECT t.table_schema, t.table_name, (SELECT count(*) FROM information_schema.columns c WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name),
 (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey::int[], a.attnum::int)) FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey)
   WHERE i.indrelid=(t.table_schema||'.'||t.table_name)::regclass AND i.indisprimary)
FROM information_schema.tables t WHERE t.table_schema IN ('erp','mes','lgs','geo') ORDER BY 1,2;
SQL
# SAP 쪽 새 표(Z) — 로컬은 SAP 대역. 없으면 ER 의 SAP 표만 비고 나머지는 그대로 만든다
podman exec -i hotdb-sap-sim psql -U sapsim -d sapsim -At -F'|' > out/sapz.txt 2>/dev/null <<'SQL' || : > out/sapz.txt
SELECT c.table_schema, c.table_name, c.ordinal_position, c.column_name, format_type(a.atttypid,a.atttypmod), c.is_nullable, coalesce(c.column_default,''), 'NO'
FROM information_schema.columns c JOIN pg_attribute a ON a.attrelid=(c.table_schema||'.'||c.table_name)::regclass AND a.attname=c.column_name
WHERE c.table_schema='erpsrc' AND c.table_name='zhotdb_actual_result' ORDER BY 3;
SQL
podman exec -i hotdb-sap-sim psql -U sapsim -d sapsim -At -F'|' > out/sapz_pk.txt 2>/dev/null <<'SQL' || : > out/sapz_pk.txt
SELECT n.nspname, c.relname, a.attname FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=ANY(i.indkey) WHERE i.indisprimary AND c.relname='zhotdb_actual_result';
SQL
for e in tsdb rdb sap; do python er.py $e > out/er-$e.html; done
python er.py --css > out/er.css
for f in all status rel; do python ~/.claude/skills/html-doc/scripts/make-flow.py "flow-$f.json" > "out/flow-$f.html"; done
python schema-map.py > out/schema-map.html
python schema-map.py --css > out/schema-map.css
python gen.py
