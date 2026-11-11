"""采集编排：保证“翻页失败不让半批记录入库”，且大批次内存有界。

流程：
1. 首次采集由调用方提供起始日；之后起始日取 harvest_progress.last_until
   （上次成功采集的截止日，含当天），实现无缝续采。
2. ListRecords 按日期拉取 oai_dc，续页仅用 resumptionToken。
3. resumptionToken 失效（badResumptionToken）时丢弃已取页，按原区间整体重抓一次。
4. 拉取过程把记录流式写入数据库旁的磁盘暂存文件（JSON Lines），内存只驻留
   当前页；65,536 条 × 4 KiB dc:description 的批次也能在 256 MiB 限制内采完。
5. 所有页取完后，在单个 SQLite 事务内逐条从暂存文件读出并 upsert/撤下书目、
   推进截止日；无匹配记录同样推进截止日。事务失败则整批回滚，进度不变，
   提交前目录查询始终返回原目录。
"""
from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
import tempfile
import threading
from dataclasses import dataclass
from typing import Iterator

from . import db
from .oai_client import OAIClient, OAIError, OAIRecord

logger = logging.getLogger("harvester")

# 进程内互斥：同一时刻只允许一个采集任务，避免进度竞争
_HARVEST_LOCK = threading.Lock()

# 每拉取多少页输出一次进度日志
_LOG_EVERY_PAGES = 64


class HarvestConflictError(RuntimeError):
    pass


@dataclass
class HarvestSummary:
    from_date: str
    until_date: str
    pages: int
    fetched: int
    inserted: int
    updated: int
    withdrawn: int
    retries: int
    run_id: int
    status: str = "success"
    note: str | None = None


class RecordSpool:
    """把已取记录暂存到磁盘（JSON Lines），避免大批次在内存中堆积。

    写阶段逐条 append；finish_writing 后用 iter_records 逐条读回，
    任意时刻内存中只驻留一条记录。discard 删除暂存文件（幂等）。
    """

    def __init__(self, directory: str) -> None:
        os.makedirs(directory, exist_ok=True)
        fd, self._path = tempfile.mkstemp(
            prefix="harvest-spool-", suffix=".jsonl", dir=directory
        )
        self._writer: io.TextIOBase | None = os.fdopen(fd, "w", encoding="utf-8")
        self.count = 0

    @property
    def path(self) -> str:
        return self._path

    def append(self, record: OAIRecord) -> None:
        assert self._writer is not None, "暂存文件已关闭写入口"
        self._writer.write(
            json.dumps(
                {
                    "identifier": record.identifier,
                    "datestamp": record.datestamp,
                    "deleted": record.deleted,
                    "dc": record.dc,
                },
                ensure_ascii=False,
            )
        )
        self._writer.write("\n")
        self.count += 1

    def extend(self, records: list[OAIRecord]) -> None:
        for record in records:
            self.append(record)

    def finish_writing(self) -> None:
        if self._writer is not None:
            self._writer.flush()
            self._writer.close()
            self._writer = None

    def iter_records(self) -> Iterator[OAIRecord]:
        self.finish_writing()
        with open(self._path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                yield OAIRecord(
                    identifier=payload["identifier"],
                    datestamp=payload["datestamp"],
                    deleted=bool(payload["deleted"]),
                    dc={key: list(values) for key, values in payload["dc"].items()},
                )

    def discard(self) -> None:
        self.finish_writing()
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass


def _db_directory(conn: sqlite3.Connection) -> str:
    """暂存文件放在数据库所在目录（容器内为 /data 卷），便于随卷清理。"""
    for _seq, name, file in conn.execute("PRAGMA database_list"):
        if name == "main" and file:
            return os.path.dirname(file) or "."
    return "."


def _fetch_full_window(client: OAIClient, from_date: str | None,
                       until_date: str,
                       spool_dir: str) -> tuple[RecordSpool, int, int]:
    """完整拉取一个日期区间，记录流式写入磁盘暂存文件。

    返回 (暂存文件, 页数, 因 badResumptionToken 触发的重抓次数)。
    调用方负责在落库完成后 spool.discard()。
    续页令牌失效时丢弃已取页，按原区间重新开始，只重抓一次。
    """
    retries = 0

    def _attempt_once() -> tuple[RecordSpool, int]:
        spool = RecordSpool(spool_dir)
        try:
            pages = 1
            page = client.list_records_first(
                from_date=from_date, until_date=until_date
            )
            spool.extend(page.records)
            token = page.resumption_token

            while token:
                page = client.list_records_resume(token)
                pages += 1
                spool.extend(page.records)
                token = page.resumption_token
                if pages % _LOG_EVERY_PAGES == 0:
                    logger.info("已拉取 %d 页 / %d 条，继续翻页", pages, spool.count)
            spool.finish_writing()
            return spool, pages
        except Exception:
            # 任何失败（含令牌失效）都先删除本次暂存，半批数据绝不留存
            spool.discard()
            raise

    try:
        spool, pages = _attempt_once()
    except OAIError as exc:
        if exc.code != "badResumptionToken":
            raise
        # 续页令牌失效：本批已取页全部作废，按原区间重抓一次；
        # 重抓期间再次失败属于不可恢复错误，直接向上抛出
        retries = 1
        logger.warning(
            "resumptionToken 失效，丢弃已取书目，按区间 %s~%s 重抓一次",
            from_date, until_date,
        )
        spool, pages = _attempt_once()

    return spool, pages, retries


def harvest(client: OAIClient, *, until_date: str,
            from_date: str | None) -> HarvestSummary:
    """执行一次采集。from_date 为 None 时从上次成功截止日续采。"""
    if not _HARVEST_LOCK.acquire(blocking=False):
        raise HarvestConflictError("已有采集任务正在执行，请稍后再试")
    try:
        return _harvest_locked(client, until_date=until_date,
                               from_date=from_date)
    finally:
        _HARVEST_LOCK.release()


def _harvest_locked(client: OAIClient, *, until_date: str,
                    from_date: str | None) -> HarvestSummary:
    conn = db.get_connection()
    run_id: int | None = None
    spool: RecordSpool | None = None
    try:
        # 起始日：显式传入优先，否则取上次成功采集的截止日
        effective_from = from_date or db.get_last_until(conn)
        run_id = db.start_run(conn, effective_from or "", until_date)
        conn.commit()

        try:
            spool, pages, retries = _fetch_full_window(
                client, effective_from, until_date, _db_directory(conn)
            )
        except OAIError as exc:
            if exc.code == "noRecordsMatch":
                # 无匹配：不写书目，但仍推进截止日
                return _commit_empty_window(
                    conn, run_id, effective_from, until_date
                )
            raise

        fetched = spool.count
        try:
            # 整批落库：单事务逐条从暂存文件读出，任何异常整体回滚，
            # 半批数据不会入库；提交前目录查询仍返回原目录
            with db.transaction(conn):
                inserted = updated = withdrawn = 0
                for record in spool.iter_records():
                    if record.deleted:
                        # 遇删除标记就撤下；库中不存在则落 withdrawn 墓碑
                        if db.withdraw_bib_record(
                            conn, identifier=record.identifier,
                            datestamp=record.datestamp,
                        ):
                            withdrawn += 1
                        else:
                            db.insert_withdrawn_tombstone(
                                conn, identifier=record.identifier,
                                datestamp=record.datestamp,
                            )
                            withdrawn += 1
                        continue
                    outcome = db.upsert_bib_record(
                        conn,
                        identifier=record.identifier,
                        datestamp=record.datestamp,
                        fields=_first_values(record.dc),
                        raw_dc=json.dumps(record.dc, ensure_ascii=False),
                    )
                    if outcome == "inserted":
                        inserted += 1
                    else:
                        updated += 1

                db.advance_progress(conn, until_date)
                db.finish_run(
                    conn, run_id, status="success", pages=pages,
                    fetched=fetched, inserted=inserted, updated=updated,
                    withdrawn=withdrawn, retries=retries, error=None,
                )
                conn.commit()
        finally:
            spool.discard()
            spool = None

        logger.info(
            "采集成功 区间=%s~%s 页=%d 取=%d 增=%d 改=%d 撤=%d 重抓=%d",
            effective_from, until_date, pages, fetched,
            inserted, updated, withdrawn, retries,
        )
        return HarvestSummary(
            from_date=effective_from or "",
            until_date=until_date,
            pages=pages,
            fetched=fetched,
            inserted=inserted,
            updated=updated,
            withdrawn=withdrawn,
            retries=retries,
            run_id=run_id,
        )
    except Exception as exc:
        # 失败：确保事务回滚，截止日不推进；另起事务记录失败历史
        conn.rollback()
        logger.exception("采集失败，已回滚本批全部变更：%s", exc)
        if run_id is not None:
            try:
                fail_conn = db.get_connection()
                try:
                    db.finish_run(
                        fail_conn, run_id, status="failed",
                        pages=0, fetched=0, inserted=0, updated=0, withdrawn=0,
                        retries=0, error=str(exc),
                    )
                    fail_conn.commit()
                finally:
                    fail_conn.close()
            except Exception:  # pragma: no cover - 历史记录失败不掩盖主错误
                logger.exception("写入失败运行历史时出错")
        raise
    finally:
        if spool is not None:
            spool.discard()
        conn.close()


def _commit_empty_window(conn, run_id: int, from_date: str | None,
                         until_date: str) -> HarvestSummary:
    with db.transaction(conn):
        db.advance_progress(conn, until_date)
        db.finish_run(
            conn, run_id, status="success", pages=0, fetched=0,
            inserted=0, updated=0, withdrawn=0, retries=0, error=None,
        )
        conn.commit()
    logger.info("区间 %s~%s 无匹配记录，截止日仍推进至 %s",
                from_date, until_date, until_date)
    return HarvestSummary(
        from_date=from_date or "",
        until_date=until_date,
        pages=0,
        fetched=0,
        inserted=0,
        updated=0,
        withdrawn=0,
        retries=0,
        run_id=run_id,
        note="区间内无匹配记录，截止日已推进",
    )


def _first_values(dc: dict) -> dict:
    """Dublin Core 多值字段取首值落列，完整多值保留在 raw_dc。"""
    return {key: (values[0] if values else None) for key, values in dc.items()}
