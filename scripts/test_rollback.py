"""故障注入：resumptionToken 永远失效，用于验证整批回滚。

单独放在 scripts/，不进入应用镜像。
运行：python3 scripts/test_rollback.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.oai_client import OAIClient, OAIError, OAIPage, OAIRecord  # noqa: E402

DB_PATH = "/tmp/e2e/data/failtest.db"


class FlakyClient(OAIClient):
    def __init__(self):  # 不建立真实 HTTP 连接
        pass

    def list_records_first(self, *, from_date, until_date):
        return OAIPage(
            records=[
                OAIRecord(
                    identifier=f"oai:fake:{i:03d}",
                    datestamp="2026-10-01",
                    deleted=False,
                    dc={"title": [f"占位书目 {i:03d}"]},
                )
                for i in range(10)  # 模拟首页已取到 10 条
            ],
            resumption_token="token-a",
            complete_list_size=27,
            cursor=0,
        )

    def list_records_resume(self, token):
        raise OAIError("badResumptionToken", "模拟令牌持续失效")


from app import db, harvester  # noqa: E402

db.DB_PATH = DB_PATH  # 指向临时库，暂存文件也会落在同一目录
db.init_db(DB_PATH)
before = db.get_last_until(db.get_connection(DB_PATH))
print("采集前 last_until:", before)

try:
    harvester.harvest(
        FlakyClient(), until_date="2026-10-27", from_date="2026-10-01"
    )
except OAIError as e:
    print("预期失败抛出:", e)

conn = db.get_connection(DB_PATH)
print("采集后 last_until（应仍为 None）:", db.get_last_until(conn))
print("书目条数（应为 0，半批未入库）:",
      conn.execute("SELECT COUNT(*) FROM bib_records").fetchone()[0])
run = conn.execute(
    "SELECT status, error FROM harvest_runs ORDER BY id DESC LIMIT 1"
).fetchone()
print("最近运行:", dict(run))
conn.close()

leftovers = [f for f in os.listdir(os.path.dirname(DB_PATH))
             if f.startswith("harvest-spool-")]
print("残留暂存文件（应为空）:", leftovers)
