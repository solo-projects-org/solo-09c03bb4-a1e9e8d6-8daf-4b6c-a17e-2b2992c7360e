"""全规模内存验证：65,536 条 × 4 KiB dc:description、每页 256 条。

在同一进程内启动样例 OAI 源并执行一次完整采集（比 Docker Compose
双容器部署更严苛：样例源与采集端共享同一份 RSS），打印峰值内存并
断言低于 256 MiB（对应 docker-compose.yml 中 harvester-api 的
mem_limit: 256m）。

运行：python3 scripts/verify_big_harvest.py [db_path]
"""
import os
import resource
import socket
import sys
import threading
from http.server import ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "sample-oai"))

RECORD_COUNT = 65_536
PAGE_SIZE = 256
DESCRIPTION_BYTES = 4 * 1024
DATE_SPAN_DAYS = 90
MEMORY_LIMIT_KIB = 256 * 1024  # 256 MiB，ru_maxrss 在 Linux 上单位为 KiB

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/e2e/data/big-harvest.db"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> int:
    import server as oai_server

    oai_server.RECORDS = oai_server._build_records(
        RECORD_COUNT, deleted_index=0, date_span_days=DATE_SPAN_DAYS
    )
    oai_server.PAGE_SIZE = PAGE_SIZE
    oai_server.DESCRIPTION_BYTES = DESCRIPTION_BYTES
    oai_server.TOKENS = oai_server.TokenStore()

    port = _free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), oai_server.Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{port}/oai"
    print(f"[verify] 样例源已启动：{RECORD_COUNT} 条 / 每页 {PAGE_SIZE} 条 / "
          f"每条描述 {DESCRIPTION_BYTES} 字节 -> {endpoint}")

    from app import db, harvester
    from app.oai_client import OAIClient

    db.DB_PATH = DB_PATH
    db.init_db(DB_PATH)

    try:
        with OAIClient(endpoint) as client:
            summary = harvester.harvest(
                client, until_date="2026-12-31", from_date="2026-10-01"
            )
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)

    peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f"[verify] 采集结果: pages={summary.pages} fetched={summary.fetched} "
          f"inserted={summary.inserted} updated={summary.updated} "
          f"withdrawn={summary.withdrawn} retries={summary.retries}")
    print(f"[verify] 峰值内存 RSS: {peak_kib / 1024:.1f} MiB "
          f"（上限 256.0 MiB）")

    conn = db.get_connection(DB_PATH)
    stored = conn.execute("SELECT COUNT(*) FROM bib_records").fetchone()[0]
    desc_ok = conn.execute(
        "SELECT COUNT(*) FROM bib_records WHERE LENGTH(description) = ?",
        (DESCRIPTION_BYTES,),
    ).fetchone()[0]
    last_until = db.get_last_until(conn)
    conn.close()
    print(f"[verify] 入库 {stored} 条，其中 4 KiB 描述 {desc_ok} 条，"
          f"last_until={last_until}")

    assert summary.fetched == RECORD_COUNT, "采入条数不符"
    assert stored == RECORD_COUNT, "入库条数不符"
    assert desc_ok == RECORD_COUNT, "4 KiB 描述未完整落库"
    assert last_until == "2026-12-31", "截止日未推进"
    assert peak_kib < MEMORY_LIMIT_KIB, (
        f"峰值内存 {peak_kib / 1024:.1f} MiB 超出 256 MiB 上限"
    )
    print("[verify] 通过：大批次在 256 MiB 预算内一次采完并整批提交")
    return 0


if __name__ == "__main__":
    sys.exit(main())
