#!/usr/bin/env python3
"""레거시 대역(SAP · Oracle) 준비 — RFC Service · DB Agent 를 로컬에서 검증할 원천 표와 시험 데이터.

    python scripts/legacy-sim.py setup [행 수=200]   # 표 만들기 + 시드 (다시 돌리면 지우고 새로)
    python scripts/legacy-sim.py mutate [행 수=5]    # 표마다 몇 행의 upd_date · upd_time 을 지금으로 바꿈

표 모양은 db/legacy-db/V2__sample_tables.sql (scripts/gen-sample-legacy.py 가 만든 시험용 표)에서 가져온다.
값도 전부 지어낸 것이다. 컨테이너는 compose.legacy-sim.yml.

  SAP 대역    : 폴링 작업이 있는 erp 3표만 (스키마 erpsrc) + RFC Service 가 쓰는 Z 표
  Oracle 대역 : 레거시 DB 의 Oracle 사본(mes · lgs · geo)과 같은 모양 — 스키마마다 Oracle 사용자(소유자) 하나,
                표 전부(64). DB Agent 는 읽기 계정 LEGACY_READER 로 읽는다 (SELECT 권한만).
                FREEPDB1 에는 이 소유자 셋과 읽기 계정만 둔다 (기본 사용자 PDBADMIN · 예전 HOTDB_SIM 은 지움)
"""
import random
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DDL = ROOT / "db" / "legacy-db" / "V2__sample_tables.sql"
SAP_SCHEMA = "erpsrc"   # SAP 대역의 원천 스키마 (RFC Service 의 SAP_SCHEMA 기본값)

# 원천 → 대역 표. RFC Service 의 application.yml · DB Agent 의 poll-jobs.yml 작업 목록과 맞춘다
SAP_TABLES = ["erp.item", "erp.part", "erp.order_line"]
ORACLE_SCHEMAS = ["mes", "lgs", "geo"]
ORACLE_POLLED = ["mes.work_log", "mes.alarm", "lgs.shipment", "lgs.tracking"]   # 통합 테스트가 쓰는 표 — 행을 많이
OTHER_ROWS = 50                                                                  # 나머지 표의 행 수


def tables(schema):
    return [f"{schema}.{t}" for t in re.findall(rf"CREATE TABLE IF NOT EXISTS {schema}\.(\w+) \(", DDL.read_text(encoding="utf-8"))]


ORACLE_TABLES = [q for s in ORACLE_SCHEMAS for q in tables(s)]


def ddl(qualified):
    schema, table = qualified.split(".")
    text = DDL.read_text(encoding="utf-8")
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {schema}\.{table} \((.*?)\n\);", text, re.S)
    if not m:
        sys.exit(f"{qualified} 의 DDL 이 {DDL.name} 에 없습니다")
    cols, pk = [], []
    for line in m.group(1).split(",\n"):
        line = line.strip()
        if line.startswith("PRIMARY KEY"):
            pk = [c.strip() for c in re.search(r"\((.*)\)", line).group(1).split(",")]
        elif line:
            parts = line.split()
            cols.append((parts[0], parts[1], "NOT NULL" in line))
    return table, cols, pk


def value(col, typ, i, now, keyed):
    if col == "upd_date":
        return now.strftime("%Y%m%d")
    if col == "upd_time":
        return now.strftime("%H%M%S")
    if typ == "numeric":
        return str(i if keyed else round(random.uniform(0, 1000), 2))
    if typ == "date":
        return (now - timedelta(days=i % 30)).strftime("%Y-%m-%d")
    return f"{col[:6].upper()}{i:05d}" if keyed else f"SIM-{col[:8]}-{random.randint(0, 999)}"


def rows(cols, pk, n):
    base = datetime.now() - timedelta(days=1)
    for i in range(1, n + 1):
        t = base + timedelta(seconds=i)
        yield [value(c, ty, i, t, c in pk) for c, ty, _ in cols]


def lit(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def run(container, cmd, sql):
    r = subprocess.run(["podman", "exec", "-i", container, *cmd], input=sql.encode("utf-8"), capture_output=True)
    out = r.stdout.decode("utf-8", "replace") + r.stderr.decode("utf-8", "replace")
    if r.returncode != 0 or re.search(r"(ERROR|ORA-\d+)", out):
        sys.exit(f"[{container}] 실패\n{out[-2000:]}")
    return out


PSQL = ["psql", "-U", "sapsim", "-d", "sapsim", "-v", "ON_ERROR_STOP=1", "-q"]
READER = "legacy_reader"   # DB Agent 의 읽기 계정 (비밀번호 = 이름, 로컬 전용)
SQLPLUS_SYS = ["sqlplus", "-s", "system/hotdb_sim_sys@//localhost:1521/FREEPDB1"]   # 사용자 · 표를 만드는 관리자


def sap_setup(n):
    out = [f"CREATE SCHEMA IF NOT EXISTS {SAP_SCHEMA};"]
    for q in SAP_TABLES:
        table, cols, pk = ddl(q)
        out.append(f"DROP TABLE IF EXISTS {SAP_SCHEMA}.{table};")
        body = ",\n  ".join(f"{c} {t}" for c, t, _ in cols)
        out.append(f"CREATE TABLE {SAP_SCHEMA}.{table} (\n  {body},\n  PRIMARY KEY ({', '.join(pk)})\n);")
        names = ", ".join(c for c, _, _ in cols)
        vals = ",\n".join("(" + ", ".join(lit(v) for v in r) + ")" for r in rows(cols, pk, n))
        out.append(f"INSERT INTO {SAP_SCHEMA}.{table} ({names}) VALUES\n{vals};")
    # RFC Service 가 쓰는 SAP 쪽 "RDB field data" 표. PK = 실적 키 + 판정 상태 (SAP 쪽 멱등 키)
    out.append(f"""
DROP TABLE IF EXISTS {SAP_SCHEMA}.zhotdb_actual_result;
CREATE TABLE {SAP_SCHEMA}.zhotdb_actual_result (
  zone text NOT NULL, hull_no text NOT NULL, block_id text NOT NULL, scan_id varchar(36) NOT NULL,
  judged_status text NOT NULL, device_id text, completed_at timestamp, block_progress_rate double precision,
  match_confidence double precision, sent_at timestamp NOT NULL,
  PRIMARY KEY (zone, hull_no, block_id, scan_id, judged_status));""")
    run("hotdb-sap-sim", PSQL, "\n".join(out))
    print(f"sap-sim: {', '.join(SAP_TABLES)} 각 {n}행 · {SAP_SCHEMA}.zhotdb_actual_result")


ORA_TYPE = {"text": "VARCHAR2(400)", "numeric": "NUMBER", "date": "DATE"}


def ora_lit(v, typ):
    if v is None:
        return "NULL"
    if typ == "date":
        return f"DATE '{v}'"
    return "'" + v.replace("'", "''") + "'" if typ != "numeric" else v


def q_ora(c):
    return '"' + c.upper() + '"'


def oracle_setup(n):
    out = ["SET FEEDBACK OFF", "WHENEVER SQLERROR EXIT FAILURE"]
    # 레거시와 무관한 사용자는 지운다 — 이미지 기본 PDBADMIN, 예전 읽기 계정 HOTDB_SIM
    for u in ("PDBADMIN", "HOTDB_SIM", READER.upper()):
        out.append(f"BEGIN EXECUTE IMMEDIATE 'DROP USER {u} CASCADE'; EXCEPTION WHEN OTHERS THEN NULL; END;\n/")
    out.append(f"CREATE USER {READER.upper()} IDENTIFIED BY {READER};")
    out.append(f"GRANT CREATE SESSION TO {READER.upper()};")
    for sch in ORACLE_SCHEMAS:
        u = sch.upper()
        out.append(f"BEGIN EXECUTE IMMEDIATE 'DROP USER {u} CASCADE'; EXCEPTION WHEN OTHERS THEN NULL; END;\n/")
        out.append(f"CREATE USER {u} IDENTIFIED BY {sch} QUOTA UNLIMITED ON USERS;")
        out.append(f"GRANT CREATE SESSION TO {u};")
    total = 0
    for q in ORACLE_TABLES:
        table, cols, pk = ddl(q)
        owner = q.split(".")[0].upper()
        body = ",\n  ".join(f"{q_ora(c)} {ORA_TYPE[t]}" + (" NOT NULL" if nn else "") for c, t, nn in cols)
        if pk:
            body += f",\n  PRIMARY KEY ({', '.join(q_ora(c) for c in pk)})"
        out.append(f"CREATE TABLE {owner}.{table} (\n  {body}\n);")
        out.append(f"GRANT SELECT ON {owner}.{table} TO {READER.upper()};")
        names = ", ".join(q_ora(c) for c, _, _ in cols)
        data = list(rows(cols, pk, n if q in ORACLE_POLLED else OTHER_ROWS))
        total += len(data)
        for k in range(0, len(data), 50):
            into = "\n".join(f"  INTO {owner}.{table} ({names}) VALUES ({', '.join(ora_lit(v, t) for v, (_, t, _) in zip(r, cols))})"
                             for r in data[k:k + 50])
            out.append(f"INSERT ALL\n{into}\nSELECT 1 FROM DUAL;")
    out += ["COMMIT;", "EXIT"]
    run("hotdb-oracle-sim", SQLPLUS_SYS, "\n".join(out))
    print(f"oracle-sim: 사용자 {' · '.join(s.upper() for s in ORACLE_SCHEMAS)} — 표 {len(ORACLE_TABLES)}개 · {total:,}행 "
          f"(통합 테스트 표 {len(ORACLE_POLLED)}개는 {n}행, 나머지 {OTHER_ROWS}행) · 읽기 계정 {READER.upper()}")


def reset_copies():
    """원천을 새로 채웠으면 레거시 DB 사본도 비우고 워터마크를 지운다 — 새 시드의 수정 시각이 예전 워터마크보다
    이르면 증분 폴링이 다시 읽지 않고, 원천에서 사라진 행은 사본에 남기 때문이다. 다음 주기에 처음부터 다시 받는다."""
    qs = SAP_TABLES + ORACLE_TABLES
    sql = f"TRUNCATE {', '.join(qs)};\nDELETE FROM ops.poll_state;"
    r = subprocess.run(["podman", "exec", "-i", "hotdb-legacy-db", "psql", "-U", "postgres", "-d", "legacy", "-v", "ON_ERROR_STOP=1", "-q"],
                       input=sql.encode("utf-8"), capture_output=True)
    if r.returncode != 0:
        print("레거시 DB 사본은 그대로 (hotdb-legacy-db 가 없거나 표가 아직 없음) — "
              + r.stderr.decode("utf-8", "replace").strip()[:200])
    else:
        print(f"legacy-db: 사본 {len(qs)}표 비움 · 폴링 상태 초기화 — 다음 주기에 처음부터 다시 받는다")


def mutate(k):
    now = datetime.now()
    d, t = now.strftime("%Y%m%d"), now.strftime("%H%M%S")
    sap = []
    for q in SAP_TABLES:
        table, cols, pk = ddl(q)
        val_col = next(c for c, ty, _ in cols if c not in pk and ty == "text" and c not in ("upd_date", "upd_time"))
        sap.append(f"UPDATE {SAP_SCHEMA}.{table} SET upd_date = '{d}', upd_time = '{t}', {val_col} = 'CHG-{t}' "
                   f"WHERE ctid IN (SELECT ctid FROM {SAP_SCHEMA}.{table} ORDER BY random() LIMIT {k});")
    run("hotdb-sap-sim", PSQL, "\n".join(sap))
    ora = ["SET FEEDBACK OFF", "WHENEVER SQLERROR EXIT FAILURE"]
    for q in ORACLE_TABLES:
        table, cols, pk = ddl(q)
        val_col = next((c for c, ty, _ in cols if c not in pk and ty == "text" and c not in ("upd_date", "upd_time")), None)
        chg = f", {q_ora(val_col)} = 'CHG-{t}'" if val_col else ""
        ora.append(f"UPDATE {q.split('.')[0].upper()}.{table} SET \"UPD_DATE\" = '{d}', \"UPD_TIME\" = '{t}'{chg} "
                   f"WHERE ROWNUM <= {k};")
    ora += ["COMMIT;", "EXIT"]
    run("hotdb-oracle-sim", SQLPLUS_SYS, "\n".join(ora))
    print(f"mutate: SAP {len(SAP_TABLES)}표 · Oracle {len(ORACLE_TABLES)}표, 표마다 {k}행 → upd {d} {t}, 값 CHG-{t}")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("setup", "mutate"):
        sys.exit(__doc__)
    n = int(sys.argv[2]) if len(sys.argv) > 2 else (200 if sys.argv[1] == "setup" else 5)
    if sys.argv[1] == "setup":
        sap_setup(n)
        oracle_setup(n)
        reset_copies()
    else:
        mutate(n)
