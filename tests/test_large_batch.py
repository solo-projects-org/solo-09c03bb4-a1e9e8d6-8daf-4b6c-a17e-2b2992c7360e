"""大批次采集：验证磁盘暂存让“65,536 条 × 4 KiB 描述”级别的批次
在有限内存内完成（测试用 2,048 条缩小规模验证同一代码路径）。"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.harvester import RecordSpool
from app.oai_client import OAIRecord

BIG_COUNT = 2048
BIG_PAGE = 256
DESC_BYTES = 4096


def _use_big_dataset(oai_mod, monkeypatch, count=BIG_COUNT):
    """把样例源切换为大批数据集：count 条、256 条/页、4 KiB 描述、无删除记录。"""
    monkeypatch.setattr(
        oai_mod, "RECORDS",
        oai_mod._build_records(count, deleted_index=0, date_span_days=90),
    )
    monkeypatch.setattr(oai_mod, "PAGE_SIZE", BIG_PAGE)
    monkeypatch.setattr(oai_mod, "DESCRIPTION_BYTES", DESC_BYTES)


def _spool_leftovers(db_mod) -> list[str]:
    db_dir = Path(db_mod.DB_PATH).parent
    return [p.name for p in db_dir.glob("harvest-spool-*")]


def test_record_spool_roundtrip(tmp_path):
    spool = RecordSpool(str(tmp_path))
    spool.append(OAIRecord(
        identifier="oai:x:1", datestamp="2026-10-01", deleted=False,
        dc={"title": ["t"], "description": ["x" * DESC_BYTES]},
    ))
    spool.append(OAIRecord(
        identifier="oai:x:2", datestamp="2026-10-02", deleted=True, dc={},
    ))
    assert spool.count == 2

    records = list(spool.iter_records())
    assert [r.identifier for r in records] == ["oai:x:1", "oai:x:2"]
    assert records[0].dc["description"] == ["x" * DESC_BYTES]
    assert records[0].deleted is False
    assert records[1].deleted is True

    path = spool.path
    spool.discard()
    spool.discard()  # 幂等
    assert not Path(path).exists()


@pytest.fixture()
def big_client(client, monkeypatch):
    api, db_mod, oai_mod = client
    _use_big_dataset(oai_mod, monkeypatch)
    return api, db_mod, oai_mod


def test_big_batch_harvest(big_client):
    api, db_mod, _oai = big_client
    resp = api.post(
        "/api/v1/harvest",
        json={"from": "2026-10-01", "until": "2026-12-31"},
    )
    assert resp.status_code == 200, resp.text
    result = resp.json()
    assert result["pages"] == BIG_COUNT // BIG_PAGE
    assert result["fetched"] == BIG_COUNT
    assert result["inserted"] == BIG_COUNT
    assert result["updated"] == 0
    assert result["withdrawn"] == 0
    assert result["retries"] == 0

    # 截止日推进、目录完整、4 KiB 描述逐条落库
    assert api.get("/api/v1/progress").json()["last_until"] == "2026-12-31"
    conn = db_mod.get_connection()
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM bib_records"
        ).fetchone()[0] == BIG_COUNT
        assert conn.execute(
            "SELECT COUNT(*) FROM bib_records WHERE LENGTH(description) = ?",
            (DESC_BYTES,),
        ).fetchone()[0] == BIG_COUNT
    finally:
        conn.close()

    # 暂存文件已清理
    assert _spool_leftovers(db_mod) == []


def test_big_batch_token_failure_refetches_once(big_client):
    api, db_mod, oai_mod = big_client
    # 下一次续页请求令牌失效：已取页作废，按原区间重抓一次后成功
    oai_mod.TOKENS.set_fail_next()
    resp = api.post(
        "/api/v1/harvest",
        json={"from": "2026-10-01", "until": "2026-12-31"},
    )
    assert resp.status_code == 200, resp.text
    result = resp.json()
    assert result["retries"] == 1
    assert result["pages"] == BIG_COUNT // BIG_PAGE
    assert result["fetched"] == BIG_COUNT
    assert result["inserted"] == BIG_COUNT
    assert api.get("/api/v1/progress").json()["last_until"] == "2026-12-31"
    assert _spool_leftovers(db_mod) == []


def test_big_batch_persistent_token_failure_rolls_back(big_client, monkeypatch):
    api, db_mod, oai_mod = big_client

    real_consume = oai_mod.TOKENS.consume

    def always_invalid(_token):
        real_consume(_token)
        return None

    monkeypatch.setattr(oai_mod.TOKENS, "consume", always_invalid)

    resp = api.post(
        "/api/v1/harvest",
        json={"from": "2026-10-01", "until": "2026-12-31"},
    )
    assert resp.status_code == 502
    assert resp.json()["detail"]["oai_error"] == "badResumptionToken"

    conn = db_mod.get_connection()
    try:
        # 半批记录不得入库，截止日不得推进
        assert conn.execute("SELECT COUNT(*) FROM bib_records").fetchone()[0] == 0
        assert db_mod.get_last_until(conn) is None
        run = conn.execute(
            "SELECT status FROM harvest_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert run["status"] == "failed"
    finally:
        conn.close()
    assert _spool_leftovers(db_mod) == []
