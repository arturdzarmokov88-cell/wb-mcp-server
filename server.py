"""
MCP-сервер для кабинета продавца Wildberries.

Claude подключается к нему как к пользовательскому коннектору.
API-ключ WB хранится только здесь, в переменных окружения.

Чтение: заказы, продажи, остатки, финансовый отчёт, цены, карточки,
отзывы, вопросы.
Запись (выключена по умолчанию, включается переменными окружения):
ответы на отзывы и вопросы, изменение цен и скидок.
Любая запись идёт в два шага: prepare_* показывает «было → станет»
и выдаёт код подтверждения, apply_change применяет изменение
только с этим кодом. Код живёт 10 минут.

Код подтверждения подписан (HMAC), поэтому работает и в бессерверном
режиме (Яндекс Serverless Containers), где экземпляры сервера
могут перезапускаться между запросами.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx2 as httpx
import uvicorn
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

# ---------------------------------------------------------------------------
# Настройки из переменных окружения
# ---------------------------------------------------------------------------

WB_TOKEN = os.environ.get("WB_API_TOKEN", "").strip()
MCP_SECRET = os.environ.get("MCP_SECRET", "").strip()
PORT = int(os.environ.get("PORT", "8080"))

ENABLE_FEEDBACK_WRITE = os.environ.get("ENABLE_FEEDBACK_WRITE", "false").lower() == "true"
ENABLE_PRICE_WRITE = os.environ.get("ENABLE_PRICE_WRITE", "false").lower() == "true"
ENABLE_ADS_WRITE = os.environ.get("ENABLE_ADS_WRITE", "false").lower() == "true"
# Максимальное изменение рекламной ставки за один раз, в процентах.
MAX_BID_CHANGE_PCT = float(os.environ.get("MAX_BID_CHANGE_PCT", "30"))
# Максимальное изменение цены за один раз, в процентах (вверх или вниз).
MAX_PRICE_CHANGE_PCT = float(os.environ.get("MAX_PRICE_CHANGE_PCT", "20"))
# Максимальная скидка, которую можно поставить через сервер.
MAX_DISCOUNT_PCT = int(os.environ.get("MAX_DISCOUNT_PCT", "70"))

# Абсолютный потолок любой рекламной ставки, в рублях (защита от опечаток).
MAX_BID_RUB = float(os.environ.get("MAX_BID_RUB", "1000"))

# История по дням: рекламу, остатки и воронку сервер сам забирает у WB и хранит у себя.
DB_PATH = os.environ.get("DB_PATH", "/data/wb.db")
ENABLE_SYNC = os.environ.get("ENABLE_SYNC", "true").lower() == "true"
SYNC_INTERVAL_HOURS = float(os.environ.get("SYNC_INTERVAL_HOURS", "3"))
# Склад, который считается «основным» остатком (в отчёте WB это сумма по всем складам).
STOCK_MAIN_WAREHOUSE = os.environ.get("STOCK_MAIN_WAREHOUSE", "Склад WB РФ")

CONFIRM_TTL_SECONDS = 600
MSK = timezone(timedelta(hours=3))  # WB считает дни по московскому времени

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx2").setLevel(logging.WARNING)
log = logging.getLogger("wb-mcp")
audit = logging.getLogger("wb-mcp.audit")

if not WB_TOKEN:
    log.error("Не задан WB_API_TOKEN")
    sys.exit(1)
if len(MCP_SECRET) < 32:
    log.error("MCP_SECRET должен быть не короче 32 символов (сгенерируйте случайную строку)")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Клиент WB API
# ---------------------------------------------------------------------------

HOSTS = {
    "statistics": "https://statistics-api.wildberries.ru",
    "analytics": "https://seller-analytics-api.wildberries.ru",
    "content": "https://content-api.wildberries.ru",
    "prices": "https://discounts-prices-api.wildberries.ru",
    "feedbacks": "https://feedbacks-api.wildberries.ru",
    "finance": "https://finance-api.wildberries.ru",
    "advert": "https://advert-api.wildberries.ru",
}

# Методы статистики разрешают 1 запрос в минуту, поэтому кешируем ответы.
_cache: dict[str, tuple[float, Any]] = {}
CACHE_TTL = 65


class WBError(Exception):
    pass


async def wb_request(
    service: str,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    body: Any = None,
    cache: bool = False,
    cache_ttl: int = CACHE_TTL,
) -> Any:
    url = HOSTS[service] + path
    key = f"{method} {url} {json.dumps(params, sort_keys=True)} {json.dumps(body, sort_keys=True)}"
    if cache and key in _cache and time.time() - _cache[key][0] < cache_ttl:
        return _cache[key][1]

    resp = None
    last_err: Exception | None = None
    # Сеть до доменов WB бывает нестабильной: повторяем при сбое соединения.
    # Повтор безопасен: при ошибке соединения запрос до WB не дошёл.
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10)) as client:
                resp = await client.request(
                    method,
                    url,
                    params={k: v for k, v in (params or {}).items() if v is not None},
                    json=body,
                    headers={"Authorization": WB_TOKEN},
                )
            break
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            last_err = e
            log.warning("Нет соединения с %s (попытка %s): %s %s", HOSTS[service], attempt + 1, type(e).__name__, e)
            await asyncio.sleep(1 + attempt)
    if resp is None:
        raise WBError(f"Нет связи с {HOSTS[service]} ({type(last_err).__name__}: {last_err}). "
                      "Сервер не смог соединиться с WB, попробуйте ещё раз через минуту.")

    if resp.status_code == 429:
        retry = resp.headers.get("X-Ratelimit-Retry", "60")
        raise WBError(f"Превышен лимит запросов WB. Повторите через {retry} сек.")
    if resp.status_code == 401:
        raise WBError("WB отклонил токен (401): токен неверный, истёк или не имеет доступа к этому разделу.")
    if resp.status_code >= 400:
        raise WBError(f"Ошибка WB {resp.status_code}: {resp.text[:500]}")

    try:
        data = resp.json() if resp.content else None
    except ValueError:
        data = resp.text
    if cache:
        _cache[key] = (time.time(), data)
    return data


def _date(value: str | None, default_days_ago: int) -> str:
    if value:
        return value
    return (datetime.now(timezone.utc) - timedelta(days=default_days_ago)).strftime("%Y-%m-%d")


def _summarize_by_article(rows: list[dict], amount_field: str) -> list[dict]:
    """Сводка по артикулам: количество и сумма."""
    agg: dict[Any, dict] = defaultdict(lambda: {"count": 0, "sum": 0.0})
    for r in rows:
        k = (r.get("nmId"), r.get("supplierArticle"))
        agg[k]["count"] += 1
        agg[k]["sum"] += float(r.get(amount_field) or 0)
    out = [
        {"nmId": k[0], "supplierArticle": k[1], "count": v["count"], "sum": round(v["sum"], 2)}
        for k, v in agg.items()
    ]
    return sorted(out, key=lambda x: x["count"], reverse=True)


# ---------------------------------------------------------------------------
# История по дням (SQLite): реклама, остатки, воронка
# ---------------------------------------------------------------------------
# WB отдаёт рекламную статистику с лимитом ~1 запрос в минуту, воронку только за
# последние 7 дней, а остатки только на текущий момент. Поэтому сервер сам
# регулярно забирает данные и складывает их в базу: любой день потом читается
# мгновенно и без лимитов.

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ad_daily (
  day TEXT NOT NULL, advert_id INTEGER NOT NULL, nm_id INTEGER NOT NULL, name TEXT,
  views REAL, clicks REAL, spend REAL, atbs REAL, orders REAL, shks REAL, sum_price REAL,
  PRIMARY KEY (day, advert_id, nm_id)
);
CREATE TABLE IF NOT EXISTS ad_campaigns (advert_id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE IF NOT EXISTS stock_daily (
  day TEXT NOT NULL, nm_id INTEGER NOT NULL, vendor_code TEXT, size TEXT NOT NULL DEFAULT '',
  on_warehouses INTEGER, in_way_to_clients INTEGER, in_way_returns INTEGER, by_warehouse TEXT,
  PRIMARY KEY (day, nm_id, size)
);
CREATE TABLE IF NOT EXISTS funnel_daily (
  day TEXT NOT NULL, nm_id INTEGER NOT NULL, vendor_code TEXT, title TEXT,
  open_count REAL, cart_count REAL, order_count REAL, order_sum REAL,
  buyout_count REAL, buyout_sum REAL, wishlist_count REAL,
  PRIMARY KEY (day, nm_id)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS pending (
  code TEXT PRIMARY KEY, action TEXT NOT NULL, payload TEXT NOT NULL,
  expires INTEGER NOT NULL, used INTEGER NOT NULL DEFAULT 0, created TEXT
);
"""

_db_conn: sqlite3.Connection | None = None
_db_lock = threading.Lock()


def _db() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        folder = os.path.dirname(DB_PATH)
        if folder:
            os.makedirs(folder, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA)
        conn.commit()
        _db_conn = conn
    return _db_conn


def _db_query(sql: str, params: tuple | list = ()) -> list[dict]:
    with _db_lock:
        return [dict(r) for r in _db().execute(sql, params).fetchall()]


def _db_write(sql: str, rows: list[tuple]) -> None:
    if not rows:
        return
    with _db_lock:
        conn = _db()
        conn.executemany(sql, rows)
        conn.commit()


async def adb_query(sql: str, params: tuple | list = ()) -> list[dict]:
    return await asyncio.to_thread(_db_query, sql, params)


async def adb_write(sql: str, rows: list[tuple]) -> None:
    await asyncio.to_thread(_db_write, sql, rows)


async def meta_get(key: str, default: str | None = None) -> str | None:
    r = await adb_query("SELECT value FROM meta WHERE key=?", (key,))
    return r[0]["value"] if r else default


async def meta_set(key: str, value: Any) -> None:
    await adb_write("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", [(key, str(value))])


def _today() -> Any:
    return datetime.now(MSK).date()


def _now_iso() -> str:
    return datetime.now(MSK).isoformat(timespec="seconds")


def _age_minutes(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        return (datetime.now(MSK) - datetime.fromisoformat(iso)).total_seconds() / 60
    except ValueError:
        return None


async def _wb_retry(*args: Any, tries: int = 4, **kwargs: Any) -> Any:
    """wb_request с ожиданием при превышении лимита WB (429)."""
    for attempt in range(tries):
        try:
            return await wb_request(*args, **kwargs)
        except WBError as e:
            text = str(e)
            if "лимит" in text and attempt < tries - 1:
                m = re.search(r"через (\d+) сек", text)
                wait = int(m.group(1)) + 2 if m else 65
                log.info("Лимит WB, жду %s сек (попытка %s)", wait, attempt + 1)
                await asyncio.sleep(min(wait, 120))
                continue
            raise


# --- загрузка из WB в базу -------------------------------------------------

_AD_FIELDS = ("views", "clicks", "sum", "atbs", "orders", "shks", "sum_price")


async def sync_ads(days_back: int) -> dict:
    """Рекламная статистика по дням, кампаниям и артикулам за последние days_back дней."""
    days_back = max(1, min(days_back, 30))
    d2 = _today()
    d1 = d2 - timedelta(days=days_back)
    adverts = await _get_adverts(statuses="7,9,11" if days_back > 7 else "9,11")
    names = {a.get("id"): (a.get("settings") or {}).get("name") for a in adverts}
    ids = [i for i in names if i]
    if not ids:
        return {"campaigns": 0, "rows": 0}
    await adb_write("INSERT OR REPLACE INTO ad_campaigns(advert_id, name) VALUES (?, ?)",
                    [(i, names[i]) for i in ids])

    agg: dict[tuple, list] = {}
    for n, start in enumerate(range(0, len(ids), 50)):
        if n:
            await asyncio.sleep(62)  # лимит fullstats: 1 запрос в минуту
        part = await _wb_retry(
            "advert", "GET", "/adv/v3/fullstats",
            params={"ids": ",".join(str(x) for x in ids[start:start + 50]),
                    "beginDate": d1.isoformat(), "endDate": d2.isoformat()},
        )
        for c in part or []:
            advert_id = c.get("advertId")
            for day in c.get("days") or []:
                day_s = str(day.get("date") or "")[:10]
                if not day_s:
                    continue
                for app in day.get("apps") or []:
                    for nm in app.get("nms") or []:
                        key = (day_s, advert_id, nm.get("nmId"))
                        row = agg.setdefault(key, [nm.get("name")] + [0.0] * len(_AD_FIELDS))
                        for j, field in enumerate(_AD_FIELDS, start=1):
                            row[j] += _f(nm.get(field))
    rows = [(k[0], k[1], k[2], v[0], *v[1:]) for k, v in agg.items()]
    await adb_write(
        "INSERT OR REPLACE INTO ad_daily(day, advert_id, nm_id, name, views, clicks, spend, atbs, orders, shks, sum_price) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    covered = await meta_get("ads_covered_from")
    if covered is None or d1.isoformat() < covered:
        await meta_set("ads_covered_from", d1.isoformat())
    await meta_set("ads_synced_at", _now_iso())
    return {"campaigns": len(ids), "rows": len(rows), "from": d1.isoformat(), "to": d2.isoformat()}


async def sync_stocks() -> dict:
    """Снимок остатков на сегодня (последний снимок за день остаётся в базе)."""
    out = await _fetch_stocks()
    if "items" not in out:
        return {"skipped": out.get("status", "отчёт не готов")}
    day = _today().isoformat()
    rows = [(day, it["nmId"], it.get("vendorCode"), it.get("size") or "", it.get("on_warehouses") or 0,
             it.get("in_way_to_clients") or 0, it.get("in_way_returns") or 0,
             json.dumps(it.get("by_warehouse") or {}, ensure_ascii=False))
            for it in out["items"] if it.get("nmId")]
    await adb_write(
        "INSERT OR REPLACE INTO stock_daily(day, nm_id, vendor_code, size, on_warehouses, in_way_to_clients, "
        "in_way_returns, by_warehouse) VALUES (?,?,?,?,?,?,?,?)", rows)
    await meta_set("stocks_synced_at", _now_iso())
    return {"rows": len(rows), "day": day}


async def sync_funnel(days_back: int = 6) -> dict:
    """Воронка продаж по дням (WB отдаёт не больше последних 7 дней, нужна подписка Джем)."""
    days_back = max(1, min(days_back, 6))
    d2 = _today()
    d1 = d2 - timedelta(days=days_back)
    cards = await _fetch_cards(1000)
    vendor = {c["nmID"]: c.get("vendorCode") for c in cards if c.get("nmID")}
    nm_ids = list(vendor)
    rows: list[tuple] = []
    for n, start in enumerate(range(0, len(nm_ids), 20)):
        if n:
            await asyncio.sleep(21)  # лимит: 3 запроса в минуту
        try:
            data = await _wb_retry(
                "analytics", "POST", "/api/analytics/v3/sales-funnel/products/history",
                body={"selectedPeriod": {"start": d1.isoformat(), "end": d2.isoformat()},
                      "nmIds": nm_ids[start:start + 20], "skipDeletedNm": True, "aggregationLevel": "day"},
            )
        except WBError as e:
            await meta_set("funnel_error", str(e))
            return {"error": str(e)}
        items = data if isinstance(data, list) else (data or {}).get("data") or []
        for it in items:
            p = it.get("product") or {}
            nm = p.get("nmId")
            for h in it.get("history") or []:
                day_s = str(h.get("date") or "")[:10]
                if not day_s or not nm:
                    continue
                rows.append((day_s, nm, p.get("vendorCode") or vendor.get(nm), p.get("title"),
                             _f(h.get("openCount")), _f(h.get("cartCount")), _f(h.get("orderCount")),
                             _f(h.get("orderSum")), _f(h.get("buyoutCount")), _f(h.get("buyoutSum")),
                             _f(h.get("addToWishlistCount"))))
    await adb_write(
        "INSERT OR REPLACE INTO funnel_daily(day, nm_id, vendor_code, title, open_count, cart_count, order_count, "
        "order_sum, buyout_count, buyout_sum, wishlist_count) VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    await meta_set("funnel_error", "")
    await meta_set("funnel_synced_at", _now_iso())
    return {"rows": len(rows), "from": d1.isoformat(), "to": d2.isoformat()}


_sync_lock = asyncio.Lock()
_sync_state: dict[str, Any] = {"running": False, "last_run": None, "results": {}, "errors": {}}


async def sync_all(ads_days: int | None = None) -> dict:
    """Полная загрузка: реклама, остатки, воронка. Ошибка одного раздела не мешает остальным."""
    if _sync_lock.locked():
        return {"status": "загрузка уже идёт"}
    async with _sync_lock:
        _sync_state["running"] = True
        try:
            if ads_days is None:
                covered = await meta_get("ads_covered_from")
                need_backfill = covered is None or covered > (_today() - timedelta(days=29)).isoformat()
                ads_days = 30 if need_backfill else 3
            steps = [("ads", lambda: sync_ads(ads_days))]
            age = _age_minutes(await meta_get("stocks_synced_at"))
            if age is None or age > 330:
                steps.append(("stocks", sync_stocks))
            steps.append(("funnel", sync_funnel))
            for name, fn in steps:
                try:
                    _sync_state["results"][name] = await fn()
                    _sync_state["errors"].pop(name, None)
                except Exception as e:  # noqa: BLE001
                    log.exception("Ошибка загрузки истории (%s)", name)
                    _sync_state["errors"][name] = f"{type(e).__name__}: {e}"
            _sync_state["last_run"] = _now_iso()
        finally:
            _sync_state["running"] = False
    return {"results": _sync_state["results"], "errors": _sync_state["errors"]}


_sync_task: asyncio.Task | None = None


async def _sync_loop() -> None:
    await asyncio.sleep(15)
    while True:
        try:
            await sync_all()
        except Exception:  # noqa: BLE001
            log.exception("Сбой фоновой загрузки")
        await asyncio.sleep(max(0.25, SYNC_INTERVAL_HOURS) * 3600)


def _ensure_sync_started() -> None:
    """Запускает фоновую загрузку (один раз). Вызывается при старте и из инструментов."""
    global _sync_task
    if not ENABLE_SYNC or (_sync_task is not None and not _sync_task.done()):
        return
    try:
        _sync_task = asyncio.get_running_loop().create_task(_sync_loop())
    except RuntimeError:
        pass


async def _refresh_ads_if_stale(max_age_min: float = 30) -> str | None:
    """Если свежих данных за последние дни давно не было, пробуем быстро обновить."""
    age = _age_minutes(await meta_get("ads_synced_at"))
    if age is not None and age < max_age_min:
        return None
    if _sync_lock.locked():
        return "Идёт фоновая загрузка данных, самые свежие цифры могут отставать на несколько часов."
    async with _sync_lock:
        try:
            await asyncio.wait_for(sync_ads(2), timeout=100)
        except Exception as e:  # noqa: BLE001
            return f"Не удалось обновить данные за последние дни ({type(e).__name__}); показаны сохранённые, они могут отставать."
    return None


# ---------------------------------------------------------------------------
# MCP-сервер
# ---------------------------------------------------------------------------

mcp = MCPServer(
    name="wildberries-seller",
    instructions=(
        "Инструменты кабинета продавца Wildberries. "
        "Изменения выполняются в два шага: prepare_* возвращает предпросмотр и код, "
        "apply_change применяет. Перед apply_change всегда показывай пользователю "
        "предпросмотр и получай его явное согласие."
    ),
)


@mcp.tool()
async def wb_check_connection() -> dict:
    """Проверить связь с WB API и права токена по каждому разделу."""
    async def ping(name: str, host: str) -> tuple[str, str]:
        err = ""
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(15, connect=8)) as client:
                    r = await client.get(host + "/ping", headers={"Authorization": WB_TOKEN})
                if r.status_code == 200:
                    return name, "OK" if attempt == 0 else f"OK (с попытки {attempt + 1})"
                if r.status_code == 401:
                    return name, "нет доступа (в токене не отмечен этот раздел)"
                if r.status_code == 429:
                    return name, "лимит проверок, повторите через 30 секунд"
                return name, f"ошибка {r.status_code}"
            except Exception as e:  # noqa: BLE001
                err = f"{type(e).__name__} {e}".strip()
                log.warning("ping %s, попытка %s: %s", host, attempt + 1, err)
                await asyncio.sleep(1)
        return name, f"нет связи после 3 попыток: {err}"

    result = dict(await asyncio.gather(*(ping(n, h) for n, h in HOSTS.items())))
    result["запись_отзывы"] = "включена" if ENABLE_FEEDBACK_WRITE else "выключена"
    result["запись_цены"] = "включена" if ENABLE_PRICE_WRITE else "выключена"
    result["запись_реклама"] = "включена" if ENABLE_ADS_WRITE else "выключена"
    return result


@mcp.tool()
async def get_orders(date_from: str | None = None, details: bool = False) -> dict:
    """Заказы начиная с даты (YYYY-MM-DD, по умолчанию 7 дней назад; WB хранит до 90 дней).
    Возвращает сводку по артикулам; details=true добавит до 200 последних строк."""
    rows = await wb_request(
        "statistics", "GET", "/api/v1/supplier/orders",
        params={"dateFrom": _date(date_from, 7)}, cache=True,
    ) or []
    cancelled = sum(1 for r in rows if r.get("isCancel"))
    out = {
        "period_from": _date(date_from, 7),
        "total_orders": len(rows),
        "cancelled": cancelled,
        "sum_price_with_disc": round(sum(float(r.get("priceWithDisc") or 0) for r in rows), 2),
        "by_article": _summarize_by_article(rows, "priceWithDisc"),
    }
    if details:
        out["rows"] = rows[-200:]
    return out


@mcp.tool()
async def get_sales(date_from: str | None = None, details: bool = False) -> dict:
    """Продажи (выкупы) и возвраты с даты (YYYY-MM-DD, по умолчанию 7 дней назад).
    Возвращает сводку; details=true добавит до 200 последних строк."""
    rows = await wb_request(
        "statistics", "GET", "/api/v1/supplier/sales",
        params={"dateFrom": _date(date_from, 7)}, cache=True,
    ) or []
    sales = [r for r in rows if str(r.get("saleID", "")).startswith("S")]
    returns = [r for r in rows if str(r.get("saleID", "")).startswith("R")]
    out = {
        "period_from": _date(date_from, 7),
        "sales_count": len(sales),
        "returns_count": len(returns),
        "for_pay_total": round(sum(float(r.get("forPay") or 0) for r in rows), 2),
        "sales_by_article": _summarize_by_article(sales, "forPay"),
    }
    if details:
        out["rows"] = rows[-200:]
    return out


async def _fetch_stocks() -> dict:
    """Отчёт об остатках от WB (формируется до минуты, кешируется на 10 минут)."""
    key = "stocks"
    if key in _cache and time.time() - _cache[key][0] < 600:
        return _cache[key][1]

    task = await wb_request(
        "analytics", "GET", "/api/v1/warehouse_remains",
        params={"groupByNm": "true", "groupBySize": "true", "groupBySa": "true"},
    )
    task_id = ((task or {}).get("data") or {}).get("taskId")
    if not task_id:
        raise WBError(f"WB не вернул ID задания на отчёт об остатках: {task}")

    for _ in range(10):  # до ~50 секунд
        await asyncio.sleep(5)
        st = await wb_request("analytics", "GET", f"/api/v1/warehouse_remains/tasks/{task_id}/status")
        status = ((st or {}).get("data") or {}).get("status")
        if status == "done":
            break
        if status in ("canceled", "purged"):
            raise WBError(f"Отчёт об остатках не сформирован (статус {status}).")
    else:
        return {"status": "отчёт ещё формируется на стороне WB, повторите запрос через минуту"}

    rows = await wb_request("analytics", "GET", f"/api/v1/warehouse_remains/tasks/{task_id}/download") or []
    items = []
    for r in rows:
        wh = {w.get("warehouseName"): w.get("quantity") for w in r.get("warehouses") or []}
        items.append({
            "nmId": r.get("nmId"),
            "vendorCode": r.get("vendorCode"),
            "size": r.get("techSize"),
            "on_warehouses": wh.pop("Всего находится на складах", 0),
            "in_way_to_clients": wh.pop("В пути до получателей", 0),
            "in_way_returns": wh.pop("В пути возвраты на склад WB", 0),
            "by_warehouse": wh,
        })
    out = {"items": items}
    _cache[key] = (time.time(), out)
    return out


@mcp.tool()
async def get_stocks() -> dict:
    """Текущие остатки на складах WB по артикулам и размерам, с разбивкой по складам
    и товарами в пути. Отчёт формируется на стороне WB, это занимает до минуты;
    результат кешируется на 10 минут. Остатки по прошлым дням: get_stock_history."""
    _ensure_sync_started()
    return await _fetch_stocks()


def _f(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


@mcp.tool()
async def get_finance_report(date_from: str, date_to: str, period: str = "weekly", details: bool = False) -> dict:
    """Детализация отчётов о реализации (финансы) за период, даты YYYY-MM-DD.
    period: weekly (еженедельные отчёты, по умолчанию) или daily (ежедневные).
    Сводка: продажи, возвраты, к перечислению, логистика, хранение, штрафы, удержания."""
    if period not in ("weekly", "daily"):
        return {"error": "period должен быть weekly или daily"}
    fields = ["rrdId", "nmId", "vendorCode", "docTypeName", "sellerOperName", "quantity",
              "retailAmount", "retailPriceWithDisc", "forPay", "deliveryService", "paidStorage",
              "penalty", "deduction", "paidAcceptance", "acquiringFee", "additionalPayment",
              "cashbackAmount", "saleDt"]
    page = await wb_request(
        "finance", "POST", "/api/finance/v1/sales-reports/detailed",
        body={"dateFrom": date_from, "dateTo": date_to, "limit": 100000, "rrdId": 0,
              "period": period, "fields": fields},
        cache=True, cache_ttl=600,
    )
    rows: list[dict] = page or []

    def s(field: str, where=lambda r: True) -> float:
        return round(sum(_f(r.get(field)) for r in rows if where(r)), 2)

    is_sale = lambda r: r.get("docTypeName") == "Продажа"  # noqa: E731
    is_return = lambda r: r.get("docTypeName") == "Возврат"  # noqa: E731
    out = {
        "period": f"{date_from} — {date_to}",
        "report_type": period,
        "rows": len(rows),
        "sales_count": int(s("quantity", is_sale)),
        "returns_count": int(s("quantity", is_return)),
        "retail_amount_sales": s("retailAmount", is_sale),
        "retail_amount_returns": s("retailAmount", is_return),
        "for_pay_sales": s("forPay", is_sale),
        "for_pay_returns": s("forPay", is_return),
        "logistics": s("deliveryService"),
        "storage": s("paidStorage"),
        "penalties": s("penalty"),
        "deductions": s("deduction"),
        "acceptance": s("paidAcceptance"),
        "wb_correction": s("additionalPayment"),
    }
    out["approx_to_transfer"] = round(
        out["for_pay_sales"] - out["for_pay_returns"] - out["logistics"] - out["storage"]
        - out["penalties"] - out["deductions"] - out["acceptance"] + out["wb_correction"], 2
    )

    by_nm: dict[Any, dict] = defaultdict(lambda: {"sales": 0, "returns": 0, "for_pay": 0.0, "logistics": 0.0})
    for r in rows:
        a = by_nm[(r.get("nmId"), r.get("vendorCode"))]
        if is_sale(r):
            a["sales"] += int(_f(r.get("quantity")))
            a["for_pay"] += _f(r.get("forPay"))
        elif is_return(r):
            a["returns"] += int(_f(r.get("quantity")))
            a["for_pay"] -= _f(r.get("forPay"))
        a["logistics"] += _f(r.get("deliveryService"))
    out["by_article"] = sorted(
        [{"nmId": k[0], "vendorCode": k[1], **{x: (round(y, 2) if isinstance(y, float) else y) for x, y in v.items()}}
         for k, v in by_nm.items() if k[0]],
        key=lambda x: x["sales"], reverse=True,
    )
    if len(rows) >= 100000:
        out["warning"] = "Строк больше 100 000, показана только первая часть. Сократите период."
    out["note"] = "approx_to_transfer — оценка; точная сумма в отчёте WB. Данные доступны с 29.01.2024."
    if details:
        out["sample_rows"] = rows[:200]
    return out


@mcp.tool()
async def get_prices(nm_id: int | None = None) -> dict:
    """Текущие цены и скидки товаров (все или один артикул WB nm_id)."""
    items: list[dict] = []
    offset = 0
    while True:
        data = await wb_request(
            "prices", "GET", "/api/v2/list/goods/filter",
            params={"limit": 1000, "offset": offset, "filterNmID": nm_id},
        )
        goods = (data or {}).get("data", {}).get("listGoods") or []
        items.extend(goods)
        if len(goods) < 1000 or nm_id:
            break
        offset += 1000
    return {"goods": items}


async def _fetch_cards(limit: int) -> list[dict]:
    cards: list[dict] = []
    cursor: dict = {"limit": min(limit, 100)}
    while len(cards) < limit:
        data = await wb_request(
            "content", "POST", "/content/v2/get/cards/list",
            body={"settings": {"cursor": cursor, "filter": {"withPhoto": -1}}},
        )
        batch = (data or {}).get("cards") or []
        for c in batch:
            cards.append({
                "nmID": c.get("nmID"),
                "vendorCode": c.get("vendorCode"),
                "title": c.get("title"),
                "brand": c.get("brand"),
                "subjectName": c.get("subjectName"),
                "photos": len(c.get("photos") or []),
                "sizes": [s.get("techSize") for s in c.get("sizes") or []],
            })
        cur = (data or {}).get("cursor") or {}
        if len(batch) < cursor["limit"] or not cur.get("nmID"):
            break
        cursor = {"limit": cursor["limit"], "updatedAt": cur.get("updatedAt"), "nmID": cur.get("nmID")}
    return cards[:limit]


@mcp.tool()
async def get_cards(limit: int = 100) -> dict:
    """Карточки товаров: артикулы, названия, бренд, предмет (до limit карточек)."""
    return {"cards": await _fetch_cards(limit)}


@mcp.tool()
async def get_feedbacks(answered: bool = False, take: int = 50, skip: int = 0, nm_id: int | None = None) -> dict:
    """Отзывы покупателей. answered=false — без ответа (по умолчанию)."""
    data = await wb_request(
        "feedbacks", "GET", "/api/v1/feedbacks",
        params={"isAnswered": str(answered).lower(), "take": min(take, 5000), "skip": skip,
                "nmId": nm_id, "order": "dateDesc"},
    )
    fb = (data or {}).get("data", {}).get("feedbacks") or []
    return {
        "count_unanswered": (data or {}).get("data", {}).get("countUnanswered"),
        "feedbacks": [
            {
                "id": f.get("id"),
                "date": f.get("createdDate"),
                "rating": f.get("productValuation"),
                "text": f.get("text"),
                "pros": f.get("pros"),
                "cons": f.get("cons"),
                "nmId": (f.get("productDetails") or {}).get("nmId"),
                "product": (f.get("productDetails") or {}).get("productName"),
                "answer": (f.get("answer") or {}).get("text") if f.get("answer") else None,
            }
            for f in fb
        ],
    }


@mcp.tool()
async def get_questions(answered: bool = False, take: int = 50, skip: int = 0) -> dict:
    """Вопросы покупателей. answered=false — без ответа (по умолчанию)."""
    data = await wb_request(
        "feedbacks", "GET", "/api/v1/questions",
        params={"isAnswered": str(answered).lower(), "take": min(take, 10000), "skip": skip, "order": "dateDesc"},
    )
    qs = (data or {}).get("data", {}).get("questions") or []
    return {
        "questions": [
            {
                "id": q.get("id"),
                "date": q.get("createdDate"),
                "text": q.get("text"),
                "nmId": (q.get("productDetails") or {}).get("nmId"),
                "product": (q.get("productDetails") or {}).get("productName"),
                "answer": (q.get("answer") or {}).get("text") if q.get("answer") else None,
            }
            for q in qs
        ]
    }


# ---------------------------------------------------------------------------
# Реклама (Продвижение): чтение
# ---------------------------------------------------------------------------

AD_STATUSES = {-1: "удалена", 4: "готова к запуску", 7: "завершена", 8: "отменена", 9: "активна", 11: "на паузе"}


async def _get_adverts(ids: list[int] | None = None, statuses: str | None = None) -> list[dict]:
    params: dict = {}
    if ids:
        params["ids"] = ",".join(str(i) for i in ids[:50])
    if statuses:
        params["statuses"] = statuses
    data = await wb_request("advert", "GET", "/api/advert/v2/adverts", params=params)
    return (data or {}).get("adverts") or []


def _ad_brief(a: dict) -> dict:
    settings = a.get("settings") or {}
    return {
        "id": a.get("id"),
        "name": settings.get("name"),
        "status": AD_STATUSES.get(a.get("status"), a.get("status")),
        "status_code": a.get("status"),
        "payment_type": {"cpm": "за показы (CPM)", "cpc": "за клики (CPC)"}.get(settings.get("payment_type"), settings.get("payment_type")),
        "bid_type": {"unified": "единая ставка", "manual": "ручная ставка"}.get(a.get("bid_type"), a.get("bid_type")),
        "placements": settings.get("placements"),
        "items": [
            {
                "nm_id": n.get("nm_id"),
                "subject": (n.get("subject") or {}).get("name"),
                "bid_search_rub": round(((n.get("bids_kopecks") or {}).get("search") or 0) / 100, 2),
                "bid_recommendations_rub": round(((n.get("bids_kopecks") or {}).get("recommendations") or 0) / 100, 2),
            }
            for n in a.get("nm_settings") or []
        ],
        "started": (a.get("timestamps") or {}).get("started"),
    }


@mcp.tool()
async def get_ad_campaigns(statuses: str = "4,9,11") -> dict:
    """Рекламные кампании: название, статус, тип оплаты и ставки по товарам.
    statuses через запятую: 4 готова к запуску, 7 завершена, 8 отменена, 9 активна, 11 пауза."""
    adverts = await _get_adverts(statuses=statuses)
    return {"campaigns": [_ad_brief(a) for a in adverts]}


def _ad_metrics(d: dict) -> dict:
    spend = _f(d.get("sum"))
    revenue = _f(d.get("sum_price"))
    return {
        "views": d.get("views"),
        "clicks": d.get("clicks"),
        "ctr_pct": d.get("ctr"),
        "cpc_rub": d.get("cpc"),
        "spend_rub": round(spend, 2),
        "add_to_cart": d.get("atbs"),
        "orders": d.get("orders"),
        "ordered_items": d.get("shks"),
        "orders_sum_rub": round(revenue, 2),
        "cr_pct": d.get("cr"),
        "drr_pct": round(spend / revenue * 100, 1) if revenue else None,
    }


_AD_SUM_SQL = ("SUM(views) views, SUM(clicks) clicks, SUM(spend) spend, SUM(atbs) atbs, "
               "SUM(orders) orders, SUM(shks) shks, SUM(sum_price) sum_price")


def _row_metrics(r: dict) -> dict:
    views, clicks, orders = r.get("views") or 0, r.get("clicks") or 0, r.get("orders") or 0
    spend = r.get("spend") or 0
    return _ad_metrics({
        "views": int(views), "clicks": int(clicks),
        "ctr": round(clicks / views * 100, 2) if views else 0,
        "cpc": round(spend / clicks, 2) if clicks else 0,
        "sum": spend, "atbs": int(r.get("atbs") or 0), "orders": int(orders),
        "shks": int(r.get("shks") or 0), "sum_price": r.get("sum_price") or 0,
        "cr": round(orders / clicks * 100, 2) if clicks else 0,
    })


async def _ad_stats_from_db(date_from: str, date_to: str, campaign_ids: list[int] | None,
                            nm_id: int | None, by_day: bool) -> dict:
    cond, params = "day BETWEEN ? AND ?", [date_from, date_to]
    if campaign_ids:
        cond += f" AND advert_id IN ({','.join('?' * len(campaign_ids))})"
        params += list(campaign_ids)
    if nm_id:
        cond += " AND nm_id = ?"
        params.append(nm_id)

    camps = await adb_query(f"SELECT advert_id, {_AD_SUM_SQL} FROM ad_daily WHERE {cond} GROUP BY advert_id", params)
    items = await adb_query(
        f"SELECT advert_id, nm_id, MAX(name) name, {_AD_SUM_SQL} FROM ad_daily WHERE {cond} "
        "GROUP BY advert_id, nm_id", params)
    names = {r["advert_id"]: r["name"] for r in await adb_query("SELECT advert_id, name FROM ad_campaigns")}
    day_rows: list[dict] = []
    item_day_rows: list[dict] = []
    if by_day:
        day_rows = await adb_query(
            f"SELECT day, advert_id, {_AD_SUM_SQL} FROM ad_daily WHERE {cond} GROUP BY day, advert_id ORDER BY day", params)
        if nm_id:
            item_day_rows = await adb_query(
                f"SELECT day, advert_id, nm_id, {_AD_SUM_SQL} FROM ad_daily WHERE {cond} "
                "GROUP BY day, advert_id, nm_id ORDER BY day", params)
    have_days = (await adb_query(f"SELECT COUNT(DISTINCT day) n FROM ad_daily WHERE {cond}", params))[0]["n"]
    want_days = (datetime.strptime(date_to, "%Y-%m-%d") - datetime.strptime(date_from, "%Y-%m-%d")).days + 1

    out = []
    total = {k: 0.0 for k in ("views", "clicks", "spend", "atbs", "orders", "shks", "sum_price")}
    for c in camps:
        for k in total:
            total[k] += c.get(k) or 0
        its = []
        for it in items:
            if it["advert_id"] != c["advert_id"]:
                continue
            row = {"nm_id": it["nm_id"], "name": it["name"], **_row_metrics(it)}
            if by_day and nm_id:
                row["by_day"] = [{"date": d["day"], **_row_metrics(d)} for d in item_day_rows
                                 if d["advert_id"] == c["advert_id"] and d["nm_id"] == it["nm_id"]]
            its.append(row)
        camp = {"campaign_id": c["advert_id"], "name": names.get(c["advert_id"]), **_row_metrics(c),
                "items": sorted(its, key=lambda x: x["spend_rub"], reverse=True)}
        if by_day:
            camp["by_day"] = [{"date": d["day"], **_row_metrics(d)} for d in day_rows if d["advert_id"] == c["advert_id"]]
        out.append(camp)
    result = {
        "period": f"{date_from} — {date_to}",
        "source": "база сервера (данные WB по дням)",
        "data_as_of": await meta_get("ads_synced_at"),
        "total": _row_metrics(total),
        "campaigns": sorted(out, key=lambda x: x["spend_rub"], reverse=True),
    }
    if have_days < want_days:
        result["note"] = (f"В базе данные за {have_days} из {want_days} дней периода "
                          "(в дни без расхода или без загрузки строк нет).")
    return result


@mcp.tool()
async def get_ad_stats(date_from: str, date_to: str, campaign_ids: list[int] | None = None,
                       by_day: bool = False, nm_id: int | None = None) -> dict:
    """Статистика рекламы за период (YYYY-MM-DD, не больше 31 дня): показы, клики, CTR, CPC,
    расход, корзины, заказы, ДРР — по кампаниям и по товарам. Без campaign_ids берутся
    все кампании с данными за период. Данные читаются из базы сервера, поэтому любой день
    доступен отдельно и без лимитов WB. by_day=true добавляет разбивку по дням для каждой
    кампании; вместе с nm_id (артикул WB) даёт дни по конкретному товару."""
    d1 = datetime.strptime(date_from, "%Y-%m-%d")
    d2 = datetime.strptime(date_to, "%Y-%m-%d")
    if (d2 - d1).days > 30 or d2 < d1:
        return {"error": "Период должен быть от 1 до 31 дня."}

    _ensure_sync_started()
    if ENABLE_SYNC:
        try:
            covered = await meta_get("ads_covered_from")
            synced = await meta_get("ads_synced_at")
        except Exception as e:  # noqa: BLE001
            log.warning("База истории недоступна: %s", e)
            covered = synced = None
        today = _today()
        if covered and synced and date_from >= covered:
            note = None
            if d2.date() >= today - timedelta(days=1):
                note = await _refresh_ads_if_stale()
            result = await _ad_stats_from_db(date_from, min(date_to, today.isoformat()), campaign_ids, nm_id, by_day)
            if note:
                result["note"] = (result.get("note", "") + " " + note).strip()
            return result
    if by_day or nm_id:
        return {"error": "Разбивка по дням и фильтр по товару доступны после первой загрузки истории "
                         "(несколько минут после запуска сервера) и для дат внутри загруженного периода. "
                         "Проверьте sync_history."}

    adverts = await _get_adverts(ids=campaign_ids) if campaign_ids else await _get_adverts(statuses="9,11")
    names = {a.get("id"): (a.get("settings") or {}).get("name") for a in adverts}
    ids = campaign_ids or list(names.keys())
    if not ids:
        return {"campaigns": [], "note": "Нет активных или приостановленных кампаний."}

    stats: list[dict] = []
    for i in range(0, len(ids), 50):
        chunk = ids[i:i + 50]
        part = await wb_request(
            "advert", "GET", "/adv/v3/fullstats",
            params={"ids": ",".join(str(x) for x in chunk), "beginDate": date_from, "endDate": date_to},
            cache=True, cache_ttl=300,
        )
        stats.extend(part or [])

    out = []
    total = defaultdict(float)
    for c in stats:
        by_nm: dict[Any, dict] = defaultdict(lambda: defaultdict(float))
        nm_names: dict[Any, str] = {}
        for day in c.get("days") or []:
            for app in day.get("apps") or []:
                for nm in app.get("nms") or []:
                    agg = by_nm[nm.get("nmId")]
                    nm_names[nm.get("nmId")] = nm.get("name")
                    for k in ("views", "clicks", "sum", "atbs", "orders", "shks", "sum_price"):
                        agg[k] += _f(nm.get(k))
        items = []
        for nm_id, agg in by_nm.items():
            views, clicks = agg["views"], agg["clicks"]
            items.append({
                "nm_id": nm_id, "name": nm_names.get(nm_id),
                **_ad_metrics({
                    "views": int(views), "clicks": int(clicks),
                    "ctr": round(clicks / views * 100, 2) if views else 0,
                    "cpc": round(agg["sum"] / clicks, 2) if clicks else 0,
                    "sum": agg["sum"], "atbs": int(agg["atbs"]), "orders": int(agg["orders"]),
                    "shks": int(agg["shks"]), "sum_price": agg["sum_price"],
                    "cr": round(agg["orders"] / clicks * 100, 2) if clicks else 0,
                }),
            })
        for k in ("views", "clicks", "sum", "atbs", "orders", "shks", "sum_price"):
            total[k] += _f(c.get(k))
        out.append({
            "campaign_id": c.get("advertId"),
            "name": names.get(c.get("advertId")),
            **_ad_metrics(c),
            "items": sorted(items, key=lambda x: x["spend_rub"], reverse=True),
        })
    clicks, views = total["clicks"], total["views"]
    return {
        "period": f"{date_from} — {date_to}",
        "total": _ad_metrics({
            "views": int(views), "clicks": int(clicks),
            "ctr": round(clicks / views * 100, 2) if views else 0,
            "cpc": round(total["sum"] / clicks, 2) if clicks else 0,
            "sum": total["sum"], "atbs": int(total["atbs"]), "orders": int(total["orders"]),
            "shks": int(total["shks"]), "sum_price": total["sum_price"],
            "cr": round(total["orders"] / clicks * 100, 2) if clicks else 0,
        }),
        "campaigns": sorted(out, key=lambda x: x["spend_rub"], reverse=True),
    }


@mcp.tool()
async def get_ad_search_queries(campaign_id: int, date_from: str, date_to: str,
                                nm_id: int | None = None, top: int = 50) -> dict:
    """Статистика по поисковым кластерам (фразам) кампании за период (YYYY-MM-DD):
    показы, клики, CTR, CPC, корзины, заказы, средняя позиция. Плюс текущие минус-фразы.
    Без nm_id берутся все товары кампании."""
    if nm_id:
        nm_ids = [nm_id]
    else:
        adverts = await _get_adverts(ids=[campaign_id])
        if not adverts:
            return {"error": f"Кампания {campaign_id} не найдена."}
        nm_ids = [n.get("nm_id") for n in adverts[0].get("nm_settings") or []][:100]
    items = [{"advert_id": campaign_id, "nm_id": n} for n in nm_ids]

    stats = await wb_request(
        "advert", "POST", "/adv/v0/normquery/stats",
        body={"from": date_from, "to": date_to, "items": items}, cache=True, cache_ttl=300,
    )
    minus = await wb_request("advert", "POST", "/adv/v0/normquery/get-minus", body={"items": items})
    minus_by_nm = {m.get("nm_id"): m.get("norm_queries") or [] for m in (minus or {}).get("items") or []}

    result = []
    for s in (stats or {}).get("stats") or []:
        phrases = sorted(s.get("stats") or [], key=lambda x: x.get("views") or 0, reverse=True)
        result.append({
            "nm_id": s.get("nm_id"),
            "phrases_total": len(phrases),
            "top_phrases": [
                {k: p.get(k) for k in ("norm_query", "views", "clicks", "ctr", "cpc", "cpm",
                                       "atbs", "orders", "avg_pos")}
                for p in phrases[:max(1, min(top, 200))]
            ],
            "minus_phrases": minus_by_nm.get(s.get("nm_id"), []),
        })
    return {
        "campaign_id": campaign_id,
        "period": f"{date_from} — {date_to}",
        "items": result,
        "note": "cpc и cpm — как их отдаёт WB (валюта указана в исходных данных).",
    }


@mcp.tool()
async def get_ad_balance() -> dict:
    """Счёт и баланс кабинета Продвижения, а также остаток бюджета по каждой
    кампании в статусах «готова к запуску», «активна» и «пауза»."""
    balance = await wb_request("advert", "GET", "/adv/v1/balance")
    adverts = await _get_adverts(statuses="4,9,11")
    names = {a.get("id"): (a.get("settings") or {}).get("name") for a in adverts}
    budgets: list[dict] = []
    ids = list(names.keys())
    for i in range(0, len(ids), 50):
        data = await wb_request("advert", "POST", "/api/advert/v2/budget", body={"advertIds": ids[i:i + 50]})
        for b in (data or {}).get("adverts") or []:
            budgets.append({"campaign_id": b.get("advertId"), "name": names.get(b.get("advertId")),
                            "budget_rub": b.get("total")})
    return {
        "account_rub": (balance or {}).get("balance"),
        "net_rub": (balance or {}).get("net"),
        "bonus_rub": (balance or {}).get("bonus"),
        "campaign_budgets": budgets,
    }


# ---------------------------------------------------------------------------
# История по дням: инструменты чтения
# ---------------------------------------------------------------------------

def _pct(a: float, b: float) -> float | None:
    return round(a / b * 100, 2) if b else None


def _funnel_metrics(r: dict) -> dict:
    opens, carts, orders = r.get("open_count") or 0, r.get("cart_count") or 0, r.get("order_count") or 0
    buyouts = r.get("buyout_count") or 0
    return {
        "opens": int(opens), "carts": int(carts), "orders": int(orders),
        "orders_sum_rub": round(r.get("order_sum") or 0),
        "buyouts": int(buyouts), "buyouts_sum_rub": round(r.get("buyout_sum") or 0),
        "wishlist": int(r.get("wishlist_count") or 0),
        "open_to_cart_pct": _pct(carts, opens),
        "cart_to_order_pct": _pct(orders, carts),
        "open_to_order_pct": _pct(orders, opens),
        "buyout_pct": _pct(buyouts, orders),
    }


_FUNNEL_SUM_SQL = ("SUM(open_count) open_count, SUM(cart_count) cart_count, SUM(order_count) order_count, "
                   "SUM(order_sum) order_sum, SUM(buyout_count) buyout_count, SUM(buyout_sum) buyout_sum, "
                   "SUM(wishlist_count) wishlist_count")


@mcp.tool()
async def get_sales_funnel(date_from: str, date_to: str, nm_ids: list[int] | None = None,
                           vendor_prefix: str | None = None, by_day: bool = False) -> dict:
    """Воронка продаж по артикулам из базы сервера: открытия карточки → корзины → заказы → выкупы
    и конверсии между шагами (YYYY-MM-DD). Фильтры: nm_ids (артикулы WB) или vendor_prefix
    (начало артикула продавца, например «LOP-» или «TRIK-»). by_day=true даёт разбивку по дням.
    WB отдаёт воронку только за последние 7 дней (и нужна подписка Джем), поэтому глубже
    начала накопления в базе данных нет. Процент выкупа по свежим дням занижен: выкупы приходят позже."""
    _ensure_sync_started()
    cond, params = "day BETWEEN ? AND ?", [date_from, date_to]
    if nm_ids:
        cond += f" AND nm_id IN ({','.join('?' * len(nm_ids))})"
        params += list(nm_ids)
    if vendor_prefix:
        cond += " AND vendor_code LIKE ?"
        params.append(vendor_prefix.replace("%", "") + "%")

    note = None
    if date_to >= _today().isoformat():
        age = _age_minutes(await meta_get("funnel_synced_at"))
        if (age is None or age > 60) and not _sync_lock.locked():
            async with _sync_lock:
                try:
                    await asyncio.wait_for(sync_funnel(1), timeout=100)
                except Exception as e:  # noqa: BLE001
                    note = f"Не удалось обновить воронку за сегодня ({type(e).__name__}); показаны сохранённые данные."

    items = await adb_query(
        f"SELECT nm_id, MAX(vendor_code) vendor_code, MAX(title) title, {_FUNNEL_SUM_SQL} FROM funnel_daily "
        f"WHERE {cond} GROUP BY nm_id ORDER BY SUM(order_count) DESC", params)
    if not items:
        err = await meta_get("funnel_error")
        return {"error": "В базе нет данных воронки за этот период.",
                "reason": err or "Загрузка воронки ещё не выполнялась или не нашла данных. См. sync_history."}
    days_total = await adb_query(
        f"SELECT day, {_FUNNEL_SUM_SQL} FROM funnel_daily WHERE {cond} GROUP BY day ORDER BY day", params)
    out_items = []
    for it in items:
        row = {"nm_id": it["nm_id"], "vendor_code": it["vendor_code"], **_funnel_metrics(it)}
        out_items.append(row)
    if by_day:
        per_day = await adb_query(
            f"SELECT day, nm_id, {_FUNNEL_SUM_SQL} FROM funnel_daily WHERE {cond} GROUP BY day, nm_id ORDER BY day", params)
        for row in out_items:
            row["by_day"] = [{"date": d["day"], **_funnel_metrics(d)} for d in per_day if d["nm_id"] == row["nm_id"]]
    total = {k: sum((it.get(k) or 0) for it in items) for k in
             ("open_count", "cart_count", "order_count", "order_sum", "buyout_count", "buyout_sum", "wishlist_count")}
    result = {
        "period": f"{date_from} — {date_to}",
        "source": "база сервера (воронка WB по дням)",
        "data_as_of": await meta_get("funnel_synced_at"),
        "days_in_db": len(days_total),
        "total": _funnel_metrics(total),
        "total_by_day": [{"date": d["day"], **_funnel_metrics(d)} for d in days_total],
        "items": out_items,
    }
    if note:
        result["note"] = note
    return result


@mcp.tool()
async def get_stock_history(date_from: str, date_to: str, nm_id: int | None = None,
                            vendor_prefix: str | None = None) -> dict:
    """Остатки по дням из базы сервера (снимки копятся с момента запуска): всего на складах,
    основной склад («Склад WB РФ», в отчёте WB это сумма по всем складам), в пути к клиентам и
    возвраты в пути. Фильтры: nm_id или vendor_prefix (начало артикула, например «LOP-»).
    Один последний снимок за день."""
    _ensure_sync_started()
    cond, params = "day BETWEEN ? AND ?", [date_from, date_to]
    if nm_id:
        cond += " AND nm_id = ?"
        params.append(nm_id)
    if vendor_prefix:
        cond += " AND vendor_code LIKE ?"
        params.append(vendor_prefix.replace("%", "") + "%")
    rows = await adb_query(
        f"SELECT day, nm_id, vendor_code, on_warehouses, in_way_to_clients, in_way_returns, by_warehouse "
        f"FROM stock_daily WHERE {cond}", params)
    if not rows:
        return {"error": "В базе нет снимков остатков за этот период.",
                "hint": "Снимки делаются при загрузке истории (раз в несколько часов). См. sync_history."}
    agg: dict[tuple, dict] = {}
    for r in rows:
        a = agg.setdefault((r["nm_id"], r["day"]), {"vendor_code": r["vendor_code"], "on": 0, "main": 0, "way": 0, "ret": 0})
        a["on"] += r["on_warehouses"] or 0
        a["way"] += r["in_way_to_clients"] or 0
        a["ret"] += r["in_way_returns"] or 0
        try:
            a["main"] += int((json.loads(r["by_warehouse"] or "{}")).get(STOCK_MAIN_WAREHOUSE) or 0)
        except (ValueError, TypeError):
            pass
    by_nm: dict[int, dict] = {}
    for (nm, day), a in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
        e = by_nm.setdefault(nm, {"nm_id": nm, "vendor_code": a["vendor_code"], "days": []})
        e["days"].append({"date": day, "on_warehouses": a["on"], "main_warehouse": a["main"],
                          "in_way_to_clients": a["way"], "in_way_returns": a["ret"]})
    items = sorted(by_nm.values(), key=lambda x: x["vendor_code"] or "")
    return {"period": f"{date_from} — {date_to}", "main_warehouse_name": STOCK_MAIN_WAREHOUSE,
            "snapshots_days": sorted({r["day"] for r in rows}), "items": items}


_bg_tasks: set[asyncio.Task] = set()


async def _history_status() -> dict:
    ads = await adb_query("SELECT MIN(day) a, MAX(day) b, COUNT(DISTINCT day) n, COUNT(*) r FROM ad_daily")
    stocks = await adb_query("SELECT MIN(day) a, MAX(day) b, COUNT(DISTINCT day) n FROM stock_daily")
    funnel = await adb_query("SELECT MIN(day) a, MAX(day) b, COUNT(DISTINCT day) n FROM funnel_daily")
    return {
        "sync_enabled": ENABLE_SYNC,
        "running": _sync_lock.locked(),
        "interval_hours": SYNC_INTERVAL_HOURS,
        "last_run": _sync_state.get("last_run"),
        "ads": {"days": ads[0]["n"], "from": ads[0]["a"], "to": ads[0]["b"], "rows": ads[0]["r"],
                "synced_at": await meta_get("ads_synced_at")},
        "stocks": {"days": stocks[0]["n"], "from": stocks[0]["a"], "to": stocks[0]["b"],
                   "synced_at": await meta_get("stocks_synced_at")},
        "funnel": {"days": funnel[0]["n"], "from": funnel[0]["a"], "to": funnel[0]["b"],
                   "synced_at": await meta_get("funnel_synced_at"), "last_error": await meta_get("funnel_error") or None},
        "last_results": _sync_state.get("results"),
        "errors": _sync_state.get("errors"),
    }


@mcp.tool()
async def sync_history(days: int = 3) -> dict:
    """Запустить загрузку истории из WB в базу сервера (реклама за последние days дней, остатки,
    воронка) и показать состояние базы. Загрузка идёт в фоне несколько минут: повторный вызов
    покажет результат. Обычно не нужна: сервер делает это сам каждые несколько часов."""
    _ensure_sync_started()
    started = False
    if not _sync_lock.locked():
        task = asyncio.get_running_loop().create_task(sync_all(ads_days=max(1, min(days, 30))))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)
        started = True
        await asyncio.sleep(0.2)
    status = await _history_status()
    status["started_now"] = started
    return status


# ---------------------------------------------------------------------------
# Запись: двухшаговое подтверждение
# ---------------------------------------------------------------------------

# Подготовленное изменение хранится на сервере (таблица pending в SQLite), а наружу
# отдаётся только короткий случайный код. Код одноразовый и живёт CONFIRM_TTL_SECONDS.
# Пережить перезапуск сервера он может, поэтому «подготовил → перезапуск → применил» работает.
_CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"  # без 0/O/1/I/L, чтобы не путать
_CODE_LEN = 6


def _db_pending_add(action: str, payload: dict) -> tuple[str, int]:
    now = int(time.time())
    expires = now + CONFIRM_TTL_SECONDS
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with _db_lock:
        conn = _db()
        # старые записи держим сутки (для разбора по журналу), потом удаляем
        conn.execute("DELETE FROM pending WHERE expires < ?", (now - 86400,))
        for _ in range(50):
            code = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(_CODE_LEN))
            try:
                conn.execute(
                    "INSERT INTO pending(code, action, payload, expires, used, created) VALUES (?,?,?,?,0,?)",
                    (code, action, body, expires, _now_iso()),
                )
                conn.commit()
                return code, expires
            except sqlite3.IntegrityError:
                continue
    raise RuntimeError("Не удалось создать код подтверждения")


def _norm_code(code: str) -> str:
    return re.sub(r"[\s\-]", "", (code or "")).upper()


def _db_pending_take(code: str) -> tuple[str, dict | None]:
    """Атомарно «берёт» код: возвращает ('ok', {a, p, e}) и помечает использованным."""
    code = _norm_code(code)
    with _db_lock:
        conn = _db()
        row = conn.execute("SELECT action, payload, expires, used FROM pending WHERE code=?", (code,)).fetchone()
        if not row:
            return "missing", None
        if row["used"]:
            return "used", None
        if row["expires"] < time.time():
            return "expired", None
        cur = conn.execute("UPDATE pending SET used=1 WHERE code=? AND used=0", (code,))
        conn.commit()
        if cur.rowcount != 1:
            return "used", None
        return "ok", {"a": row["action"], "p": json.loads(row["payload"]), "e": row["expires"]}


async def _new_confirmation(action: str, payload: dict, preview: dict) -> dict:
    code, _ = await asyncio.to_thread(_db_pending_add, action, payload)
    audit.info("PREPARED %s code=%s %s", action, code, json.dumps(payload, ensure_ascii=False))
    return {
        "preview": preview,
        "confirmation_code": code,
        "expires_in_minutes": CONFIRM_TTL_SECONDS // 60,
        "next_step": "Покажи предпросмотр пользователю. Только после его явного согласия вызови apply_change с этим кодом.",
    }


@mcp.tool()
async def prepare_feedback_answer(feedback_id: str, text: str) -> dict:
    """Шаг 1: подготовить ответ на отзыв. Ничего не отправляет, возвращает предпросмотр и код."""
    if not ENABLE_FEEDBACK_WRITE:
        return {"error": "Запись ответов выключена (ENABLE_FEEDBACK_WRITE=false)."}
    if not 2 <= len(text) <= 5000:
        return {"error": "Длина ответа должна быть от 2 до 5000 символов."}
    return await _new_confirmation(
        "feedback_answer",
        {"id": feedback_id, "text": text},
        {"отзыв": feedback_id, "ответ": text},
    )


@mcp.tool()
async def prepare_question_answer(question_id: str, text: str) -> dict:
    """Шаг 1: подготовить ответ на вопрос покупателя. Ничего не отправляет."""
    if not ENABLE_FEEDBACK_WRITE:
        return {"error": "Запись ответов выключена (ENABLE_FEEDBACK_WRITE=false)."}
    if not 2 <= len(text) <= 5000:
        return {"error": "Длина ответа должна быть от 2 до 5000 символов."}
    return await _new_confirmation(
        "question_answer",
        {"id": question_id, "text": text},
        {"вопрос": question_id, "ответ": text},
    )


@mcp.tool()
async def prepare_price_change(nm_id: int, new_price: int | None = None, new_discount: int | None = None) -> dict:
    """Шаг 1: подготовить изменение цены (до скидки, в рублях) и/или скидки (%) для артикула.
    Проверяет лимиты, показывает «было → станет». Ничего не меняет."""
    if not ENABLE_PRICE_WRITE:
        return {"error": "Изменение цен выключено (ENABLE_PRICE_WRITE=false)."}
    if new_price is None and new_discount is None:
        return {"error": "Укажите new_price и/или new_discount."}

    data = await wb_request("prices", "GET", "/api/v2/list/goods/filter",
                            params={"limit": 10, "offset": 0, "filterNmID": nm_id})
    goods = (data or {}).get("data", {}).get("listGoods") or []
    if not goods:
        return {"error": f"Товар {nm_id} не найден в ценах."}
    g = goods[0]
    sizes = g.get("sizes") or [{}]
    old_price = int(sizes[0].get("price") or 0)
    old_discount = int(g.get("discount") or 0)

    price = new_price if new_price is not None else old_price
    discount = new_discount if new_discount is not None else old_discount
    old_final = old_price * (100 - old_discount) / 100
    new_final = price * (100 - discount) / 100

    if discount < 0 or discount > MAX_DISCOUNT_PCT:
        return {"error": f"Скидка {discount}% вне допустимого диапазона 0–{MAX_DISCOUNT_PCT}%."}
    if old_final > 0:
        change = abs(new_final - old_final) / old_final * 100
        if change > MAX_PRICE_CHANGE_PCT:
            return {"error": f"Итоговая цена меняется на {change:.1f}%, лимит {MAX_PRICE_CHANGE_PCT}%. "
                             "Разбейте изменение на шаги или поднимите MAX_PRICE_CHANGE_PCT."}

    payload: dict = {"nmID": nm_id}
    if new_price is not None:
        payload["price"] = price
    if new_discount is not None:
        payload["discount"] = discount
    return await _new_confirmation(
        "price_change",
        payload,
        {
            "артикул": nm_id,
            "цена до скидки": f"{old_price} → {price}",
            "скидка": f"{old_discount}% → {discount}%",
            "итоговая цена (без СПП)": f"{old_final:.0f} → {new_final:.0f}",
        },
    )


@mcp.tool()
async def prepare_ad_status(campaign_id: int, action: str) -> dict:
    """Шаг 1: подготовить запуск (action=start) или паузу (action=pause) рекламной кампании.
    Ничего не меняет, возвращает предпросмотр и код."""
    if not ENABLE_ADS_WRITE:
        return {"error": "Управление рекламой выключено (ENABLE_ADS_WRITE=false)."}
    if action not in ("start", "pause"):
        return {"error": "action должен быть start или pause."}
    adverts = await _get_adverts(ids=[campaign_id])
    if not adverts:
        return {"error": f"Кампания {campaign_id} не найдена."}
    a = adverts[0]
    status = a.get("status")
    if action == "start" and status not in (4, 11):
        return {"error": f"Запустить можно только кампанию «готова к запуску» или «на паузе», сейчас: {AD_STATUSES.get(status, status)}."}
    if action == "pause" and status != 9:
        return {"error": f"Поставить на паузу можно только активную кампанию, сейчас: {AD_STATUSES.get(status, status)}."}
    return await _new_confirmation(
        "ad_status",
        {"id": campaign_id, "action": action},
        {
            "кампания": f"{campaign_id} «{(a.get('settings') or {}).get('name')}»",
            "действие": "запуск" if action == "start" else "пауза",
            "статус": f"{AD_STATUSES.get(status, status)} → {'активна' if action == 'start' else 'на паузе'}",
        },
    )


@mcp.tool()
async def prepare_ad_bid(campaign_id: int, nm_id: int, new_bid_rub: float, placement: str | None = None) -> dict:
    """Шаг 1: подготовить изменение ставки товара в кампании (в рублях).
    Для ручной ставки укажите placement: search (поиск) или recommendations (рекомендации);
    для единой ставки placement не нужен. Ничего не меняет."""
    if not ENABLE_ADS_WRITE:
        return {"error": "Управление рекламой выключено (ENABLE_ADS_WRITE=false)."}
    if new_bid_rub <= 0:
        return {"error": "Ставка должна быть больше нуля."}
    adverts = await _get_adverts(ids=[campaign_id])
    if not adverts:
        return {"error": f"Кампания {campaign_id} не найдена."}
    a = adverts[0]
    if a.get("status") not in (4, 9, 11):
        return {"error": f"Ставку можно менять только у кампаний в статусах 4, 9, 11, сейчас: {AD_STATUSES.get(a.get('status'))}."}
    nm = next((n for n in a.get("nm_settings") or [] if n.get("nm_id") == nm_id), None)
    if not nm:
        return {"error": f"Товара {nm_id} нет в кампании {campaign_id}."}
    bids = nm.get("bids_kopecks") or {}
    if a.get("bid_type") == "unified":
        placement = "combined"
        current = max(bids.get("search") or 0, bids.get("recommendations") or 0)
    else:
        if placement not in ("search", "recommendations"):
            return {"error": "У кампании ручная ставка: укажите placement search или recommendations."}
        current = bids.get(placement) or 0
    new_kop = int(round(new_bid_rub * 100))
    if current:
        change = abs(new_kop - current) / current * 100
        if change > MAX_BID_CHANGE_PCT:
            return {"error": f"Ставка меняется на {change:.0f}%, лимит {MAX_BID_CHANGE_PCT:.0f}%. "
                             "Разбейте изменение на шаги или поднимите MAX_BID_CHANGE_PCT."}
    place_ru = {"combined": "поиск и рекомендации", "search": "поиск", "recommendations": "рекомендации"}[placement]
    return await _new_confirmation(
        "ad_bid",
        {"advert_id": campaign_id, "nm_id": nm_id, "bid_kopecks": new_kop, "placement": placement},
        {
            "кампания": f"{campaign_id} «{(a.get('settings') or {}).get('name')}»",
            "товар": nm_id,
            "размещение": place_ru,
            "ставка, ₽": f"{current / 100:.2f} → {new_kop / 100:.2f}",
        },
    )


@mcp.tool()
async def prepare_ad_minus(campaign_id: int, nm_id: int, add: list[str] | None = None,
                           remove: list[str] | None = None) -> dict:
    """Шаг 1: подготовить изменение минус-фраз товара в кампании: добавить (add) и/или
    убрать (remove) фразы. Ничего не меняет, показывает итоговый список."""
    if not ENABLE_ADS_WRITE:
        return {"error": "Управление рекламой выключено (ENABLE_ADS_WRITE=false)."}
    add = [p.strip() for p in (add or []) if p.strip()]
    remove = [p.strip() for p in (remove or []) if p.strip()]
    if not add and not remove:
        return {"error": "Укажите фразы в add и/или remove."}
    data = await wb_request("advert", "POST", "/adv/v0/normquery/get-minus",
                            body={"items": [{"advert_id": campaign_id, "nm_id": nm_id}]})
    current = []
    for m in (data or {}).get("items") or []:
        if m.get("nm_id") == nm_id:
            current = m.get("norm_queries") or []
    new = [p for p in current if p not in remove] + [p for p in add if p not in current]
    if len(new) > 1000:
        return {"error": "У товара может быть не больше 1000 минус-фраз."}
    if current and not new:
        return {"error": "Такое изменение удалит все минус-фразы. Если это нужно, подтвердите отдельно — "
                         "уберите фразы по частям, оставив хотя бы одну, или попросите пользователя явно."}
    return await _new_confirmation(
        "ad_minus",
        {"advert_id": campaign_id, "nm_id": nm_id, "norm_queries": new},
        {
            "кампания": campaign_id,
            "товар": nm_id,
            "добавить": [p for p in add if p not in current],
            "убрать": [p for p in remove if p in current],
            "минус-фраз было → станет": f"{len(current)} → {len(new)}",
        },
    )


# --- ставки по фразам ------------------------------------------------------

def _bid_kop(row: dict) -> int | None:
    """Ставка строки из get-bids в копейках."""
    if row.get("bid_kopecks") is not None:
        return int(row["bid_kopecks"])
    return None


async def _get_phrase_bids(campaign_id: int, nm_id: int) -> list[dict]:
    data = await wb_request("advert", "POST", "/adv/v0/normquery/get-bids",
                            body={"items": [{"advert_id": campaign_id, "nm_id": nm_id}]})
    return (data or {}).get("bids") or []


async def _phrase_bid_unit() -> str:
    """В чём WB принимает поле bid при записи: kopecks или rub. Определяется по факту (см. apply)."""
    saved = await meta_get("phrase_bid_unit")
    return saved or os.environ.get("NORMQUERY_BID_UNIT", "rub")


def _to_unit(kopecks: int, unit: str) -> int | float:
    if unit == "kopecks":
        return kopecks
    return kopecks // 100 if kopecks % 100 == 0 else round(kopecks / 100, 2)


@mcp.tool()
async def get_ad_phrase_bids(campaign_id: int, nm_id: int | None = None) -> dict:
    """Текущие ставки по поисковым фразам (кластерам) в кампании. Работает у кампаний с ручной ставкой
    и оплатой за показы. Без nm_id берутся товары кампании (не больше 20). Фразы без своей ставки
    используют ставку товара на поиске (bid_search_rub)."""
    adverts = await _get_adverts(ids=[campaign_id])
    if not adverts:
        return {"error": f"Кампания {campaign_id} не найдена."}
    a = adverts[0]
    nm_settings = a.get("nm_settings") or []
    if nm_id:
        nm_settings = [n for n in nm_settings if n.get("nm_id") == nm_id]
        if not nm_settings:
            return {"error": f"Товара {nm_id} нет в кампании {campaign_id}."}
    result = []
    for n in nm_settings[:20]:
        bids = await _get_phrase_bids(campaign_id, n.get("nm_id"))
        phrases = sorted(
            ({"norm_query": b.get("norm_query"),
              "bid_rub": round((_bid_kop(b) or 0) / 100, 2)} for b in bids),
            key=lambda x: x["bid_rub"], reverse=True)
        result.append({
            "nm_id": n.get("nm_id"),
            "bid_search_rub": round(((n.get("bids_kopecks") or {}).get("search") or 0) / 100, 2),
            "phrases_with_bid": len(phrases),
            "phrases": phrases,
        })
    return {"campaign_id": campaign_id, "name": (a.get("settings") or {}).get("name"),
            "bid_type": a.get("bid_type"), "payment_type": (a.get("settings") or {}).get("payment_type"),
            "items": result}


def _phrase_campaign_error(a: dict) -> str | None:
    if a.get("status") not in (4, 9, 11):
        return f"Ставки меняются только у кампаний в статусах 4, 9, 11, сейчас: {AD_STATUSES.get(a.get('status'), a.get('status'))}."
    if a.get("bid_type") != "manual" or (a.get("settings") or {}).get("payment_type") != "cpm":
        return ("Ставки по фразам WB позволяет менять только у кампаний с ручной ставкой и оплатой за показы (CPM). "
                f"У этой кампании: {_ad_brief(a)['bid_type']}, {_ad_brief(a)['payment_type']}.")
    return None


@mcp.tool()
async def prepare_ad_phrase_bids(campaign_id: int, nm_id: int, changes: list[dict]) -> dict:
    """Шаг 1: подготовить изменение ставок по фразам товара. changes — список вида
    [{"norm_query": "шапка женская зимняя", "bid_rub": 120}, ...] (до 50). Ставка в рублях.
    Ничего не меняет, показывает «было → станет» и выдаёт код подтверждения."""
    if not ENABLE_ADS_WRITE:
        return {"error": "Управление рекламой выключено (ENABLE_ADS_WRITE=false)."}
    if not changes or len(changes) > 50:
        return {"error": "Укажите от 1 до 50 фраз в changes."}
    adverts = await _get_adverts(ids=[campaign_id])
    if not adverts:
        return {"error": f"Кампания {campaign_id} не найдена."}
    a = adverts[0]
    err = _phrase_campaign_error(a)
    if err:
        return {"error": err}
    nm = next((n for n in a.get("nm_settings") or [] if n.get("nm_id") == nm_id), None)
    if not nm:
        return {"error": f"Товара {nm_id} нет в кампании {campaign_id}."}
    item_bid = (nm.get("bids_kopecks") or {}).get("search") or 0
    current = {b.get("norm_query"): _bid_kop(b) for b in await _get_phrase_bids(campaign_id, nm_id)}

    rows, preview_rows = [], []
    seen: set[str] = set()
    for ch in changes:
        q = str((ch or {}).get("norm_query") or "").strip()
        try:
            rub = float((ch or {}).get("bid_rub"))
        except (TypeError, ValueError):
            return {"error": f"Для фразы «{q}» не задана ставка bid_rub."}
        if not q or q in seen:
            return {"error": "Фраза пустая или указана дважды."}
        seen.add(q)
        if rub <= 0 or rub > MAX_BID_RUB:
            return {"error": f"Ставка для «{q}» должна быть больше 0 и не больше {MAX_BID_RUB:.0f} ₽."}
        new_kop = int(round(rub * 100))
        own = current.get(q)
        base = own or item_bid
        if base:
            change = abs(new_kop - base) / base * 100
            if change > MAX_BID_CHANGE_PCT:
                return {"error": f"Ставка «{q}» меняется на {change:.0f}%, лимит {MAX_BID_CHANGE_PCT:.0f}%. "
                                 "Разбейте изменение на шаги."}
        rows.append({"q": q, "k": new_kop, "o": own})
        was = f"{own / 100:.2f}" if own else (f"нет своей (общая {item_bid / 100:.2f})" if item_bid else "нет")
        preview_rows.append(f"{q}: {was} → {new_kop / 100:.2f}")
    return await _new_confirmation(
        "ad_phrase_bids",
        {"advert_id": campaign_id, "nm_id": nm_id, "bids": rows},
        {"кампания": f"{campaign_id} «{(a.get('settings') or {}).get('name')}»", "товар": nm_id,
         "ставки по фразам, ₽ (было → станет)": preview_rows},
    )


@mcp.tool()
async def prepare_ad_phrase_bids_delete(campaign_id: int, nm_id: int, norm_queries: list[str]) -> dict:
    """Шаг 1: подготовить удаление собственных ставок у фраз (фраза вернётся на общую ставку товара).
    Ничего не меняет."""
    if not ENABLE_ADS_WRITE:
        return {"error": "Управление рекламой выключено (ENABLE_ADS_WRITE=false)."}
    queries = [q.strip() for q in norm_queries or [] if q and q.strip()]
    if not queries or len(queries) > 50:
        return {"error": "Укажите от 1 до 50 фраз."}
    adverts = await _get_adverts(ids=[campaign_id])
    if not adverts:
        return {"error": f"Кампания {campaign_id} не найдена."}
    a = adverts[0]
    err = _phrase_campaign_error(a)
    if err:
        return {"error": err}
    current = {b.get("norm_query"): _bid_kop(b) for b in await _get_phrase_bids(campaign_id, nm_id)}
    missing = [q for q in queries if q not in current]
    if missing:
        return {"error": f"У этих фраз нет собственной ставки: {missing}"}
    return await _new_confirmation(
        "ad_phrase_bids_delete",
        {"advert_id": campaign_id, "nm_id": nm_id, "queries": queries},
        {"кампания": f"{campaign_id} «{(a.get('settings') or {}).get('name')}»", "товар": nm_id,
         "убрать свою ставку (вернётся общая)": [f"{q}: {current[q] / 100:.2f} ₽" for q in queries if current.get(q)]},
    )


async def _apply_phrase_bids(payload: dict) -> dict:
    """Записывает ставки по фразам и сверяет результат с WB.

    В документации WB не указано, в чём поле bid (копейки или рубли), поэтому после записи
    значения читаются обратно. Если WB понял число ровно в 100 раз иначе, ставка тут же
    исправляется в нужных единицах, а единицы запоминаются."""
    advert_id, nm_id, bids = payload["advert_id"], payload["nm_id"], payload["bids"]
    wanted = {b["q"]: b["k"] for b in bids}

    async def send(pairs: dict[str, int], unit: str) -> None:
        r = await wb_request("advert", "POST", "/adv/v0/normquery/bids", body={"bids": [
            {"advert_id": advert_id, "nm_id": nm_id, "norm_query": q, "bid": _to_unit(k, unit)}
            for q, k in pairs.items()]})
        log.info("WB normquery/bids unit=%s pairs=%s resp=%s", unit, pairs, r)
        failed = (r or {}).get("failed") or [] if isinstance(r, dict) else []
        if failed:
            raise WBError("WB отклонил ставки: " + "; ".join(
                f"{f.get('norm_query')}: {f.get('reason')}" for f in failed))

    async def readback() -> dict[str, int | None]:
        await asyncio.sleep(2)
        return {b.get("norm_query"): _bid_kop(b) for b in await _get_phrase_bids(advert_id, nm_id)}

    unit = await _phrase_bid_unit()
    try:
        await send(wanted, unit)
    except WBError as e:
        if "Ошибка WB 400" not in str(e):
            raise
        other = "rub" if unit == "kopecks" else "kopecks"
        await send(wanted, other)  # если и это не принято, ошибка уйдёт наверх
        unit = other
        await meta_set("phrase_bid_unit", unit)

    got = await readback()
    bad = {q: k for q, k in wanted.items() if got.get(q) != k}
    if bad:  # возможно, запись ещё не отразилась: читаем ещё раз
        got = await readback()
        bad = {q: k for q, k in wanted.items() if got.get(q) != k}
    notes: list[str] = []
    unit_errors = {q: k for q, k in bad.items() if got.get(q) and (got[q] == k * 100 or got[q] * 100 == k)}
    if unit_errors:
        sample_q, sample_k = next(iter(unit_errors.items()))
        fixed_unit = "rub" if got[sample_q] == sample_k * 100 else "kopecks"
        notes.append(f"WB принял ставку в других единицах, исправлено (единицы записи: {fixed_unit}).")
        await send(unit_errors, fixed_unit)
        await meta_set("phrase_bid_unit", fixed_unit)
        got = await readback()
        bad = {q: k for q, k in wanted.items() if got.get(q) != k}
        still = {q: k for q, k in bad.items() if q in unit_errors}
        if still:  # не вышло: убираем собственные ставки, чтобы фразы не остались с неверной ставкой
            await wb_request("advert", "DELETE", "/adv/v0/normquery/bids", body={"bids": [
                {"advert_id": advert_id, "nm_id": nm_id, "norm_query": q} for q in still]})
            notes.append("ВНИМАНИЕ: исправить не удалось, собственные ставки этих фраз удалены "
                         f"(они вернулись на общую ставку товара): {list(still)}")
    result = {
        "status": "выполнено" if not bad else "применено частично или ещё не отразилось",
        "ставки после записи, ₽": {q: (got[q] / 100 if got.get(q) else None) for q in wanted},
    }
    pending = [q for q in bad if q not in unit_errors]
    if pending:
        notes.append(f"WB пока не показывает новую ставку у фраз {pending}; проверьте get_ad_phrase_bids через минуту.")
    if notes:
        result["notes"] = notes
    return result


# --- ставки по товарам пакетом, состав кампании ----------------------------

@mcp.tool()
async def prepare_ad_bids(campaign_id: int, bids: list[dict]) -> dict:
    """Шаг 1: подготовить изменение ставок сразу у нескольких товаров кампании одним подтверждением.
    bids — [{"nm_id": 123, "bid_rub": 110, "placement": "recommendations"}, ...] (до 50).
    placement нужен только у кампаний с ручной ставкой (search или recommendations).
    Ничего не меняет."""
    if not ENABLE_ADS_WRITE:
        return {"error": "Управление рекламой выключено (ENABLE_ADS_WRITE=false)."}
    if not bids or len(bids) > 50:
        return {"error": "Укажите от 1 до 50 товаров в bids."}
    adverts = await _get_adverts(ids=[campaign_id])
    if not adverts:
        return {"error": f"Кампания {campaign_id} не найдена."}
    a = adverts[0]
    if a.get("status") not in (4, 9, 11):
        return {"error": f"Ставку можно менять только у кампаний в статусах 4, 9, 11, сейчас: {AD_STATUSES.get(a.get('status'))}."}
    unified = a.get("bid_type") == "unified"
    by_nm = {n.get("nm_id"): n for n in a.get("nm_settings") or []}
    nm_bids, preview_rows, seen = [], [], set()
    for b in bids:
        try:
            nm_id = int((b or {}).get("nm_id"))
            rub = float((b or {}).get("bid_rub"))
        except (TypeError, ValueError):
            return {"error": "У каждого товара нужны nm_id и bid_rub."}
        if nm_id not in by_nm:
            return {"error": f"Товара {nm_id} нет в кампании {campaign_id}."}
        placement = "combined" if unified else (b or {}).get("placement")
        if not unified and placement not in ("search", "recommendations"):
            return {"error": f"Товар {nm_id}: у кампании ручная ставка, укажите placement search или recommendations."}
        if (nm_id, placement) in seen:
            return {"error": f"Товар {nm_id} указан дважды."}
        seen.add((nm_id, placement))
        if rub <= 0 or rub > MAX_BID_RUB:
            return {"error": f"Ставка товара {nm_id} должна быть больше 0 и не больше {MAX_BID_RUB:.0f} ₽."}
        cur_bids = by_nm[nm_id].get("bids_kopecks") or {}
        current = max(cur_bids.get("search") or 0, cur_bids.get("recommendations") or 0) if unified else (cur_bids.get(placement) or 0)
        new_kop = int(round(rub * 100))
        if current:
            change = abs(new_kop - current) / current * 100
            if change > MAX_BID_CHANGE_PCT:
                return {"error": f"Ставка товара {nm_id} меняется на {change:.0f}%, лимит {MAX_BID_CHANGE_PCT:.0f}%. "
                                 "Разбейте изменение на шаги."}
        nm_bids.append({"nm_id": nm_id, "bid_kopecks": new_kop, "placement": placement})
        preview_rows.append(f"{nm_id}: {current / 100:.2f} → {new_kop / 100:.2f}")
    return await _new_confirmation(
        "ad_bids",
        {"advert_id": campaign_id, "nm_bids": nm_bids},
        {"кампания": f"{campaign_id} «{(a.get('settings') or {}).get('name')}»",
         "ставки, ₽ (было → станет)": preview_rows},
    )


@mcp.tool()
async def prepare_ad_items(campaign_id: int, add: list[int] | None = None, delete: list[int] | None = None) -> dict:
    """Шаг 1: подготовить добавление (add) и/или удаление (delete) товаров в кампании по артикулам WB
    (nm_id). Добавленному товару WB ставит текущую минимальную ставку. Нельзя убрать все товары.
    Ничего не меняет, показывает итоговый состав."""
    if not ENABLE_ADS_WRITE:
        return {"error": "Управление рекламой выключено (ENABLE_ADS_WRITE=false)."}
    add = sorted({int(x) for x in add or []})
    delete = sorted({int(x) for x in delete or []})
    if not add and not delete:
        return {"error": "Укажите товары в add и/или delete."}
    if set(add) & set(delete):
        return {"error": "Один и тот же товар нельзя одновременно добавить и удалить."}
    if len(add) + len(delete) > 50:
        return {"error": "За один раз можно изменить не больше 50 товаров."}
    adverts = await _get_adverts(ids=[campaign_id])
    if not adverts:
        return {"error": f"Кампания {campaign_id} не найдена."}
    a = adverts[0]
    if a.get("status") not in (4, 9, 11):
        return {"error": f"Состав можно менять только у кампаний в статусах 4, 9, 11, сейчас: {AD_STATUSES.get(a.get('status'))}."}
    current = {n.get("nm_id"): n for n in a.get("nm_settings") or []}
    not_in = [x for x in delete if x not in current]
    if not_in:
        return {"error": f"Этих товаров нет в кампании: {not_in}"}
    already = [x for x in add if x in current]
    if already:
        return {"error": f"Эти товары уже в кампании: {already}"}
    if len(current) - len(delete) + len(add) < 1:
        return {"error": "Так в кампании не останется товаров. Если кампания не нужна, поставьте её на паузу."}

    def brief(nm_id: int) -> str:
        n = current.get(nm_id) or {}
        bids = n.get("bids_kopecks") or {}
        bid = max(bids.get("search") or 0, bids.get("recommendations") or 0)
        return f"{nm_id} (ставка {bid / 100:.2f} ₽)" if bid else str(nm_id)

    return await _new_confirmation(
        "ad_items",
        {"advert_id": campaign_id, "add": add, "delete": delete},
        {"кампания": f"{campaign_id} «{(a.get('settings') or {}).get('name')}»",
         "убрать из кампании": [brief(x) for x in delete],
         "добавить в кампанию": add,
         "товаров было → станет": f"{len(current)} → {len(current) - len(delete) + len(add)}",
         "важно": "у добавленных товаров будет текущая минимальная ставка WB; лимит WB на этот метод — 2 запроса в час."},
    )


@mcp.tool()
async def apply_change(confirmation_code: str) -> dict:
    """Шаг 2: применить подготовленное изменение. Вызывать ТОЛЬКО после явного согласия пользователя."""
    state, item = await asyncio.to_thread(_db_pending_take, confirmation_code)
    if state == "used":
        return {"error": "Этот код уже использован. Подготовьте изменение заново."}
    if state != "ok" or not item:
        return {"error": "Код неверный или истёк. Подготовьте изменение заново."}
    confirmation_code = _norm_code(confirmation_code)  # для журнала

    action, payload = item["a"], item["p"]
    try:
        if action == "feedback_answer":
            if not ENABLE_FEEDBACK_WRITE:
                return {"error": "Запись ответов выключена."}
            await wb_request("feedbacks", "POST", "/api/v1/feedbacks/answer", body=payload)
        elif action == "question_answer":
            if not ENABLE_FEEDBACK_WRITE:
                return {"error": "Запись ответов выключена."}
            await wb_request("feedbacks", "PATCH", "/api/v1/questions",
                             body={"id": payload["id"], "answer": {"text": payload["text"]}, "state": "wbRu"})
        elif action == "price_change":
            if not ENABLE_PRICE_WRITE:
                return {"error": "Изменение цен выключено."}
            res = await wb_request("prices", "POST", "/api/v2/upload/task", body={"data": [payload]})
            upload_id = (res or {}).get("data", {}).get("id")
            audit.info("APPLIED %s code=%s upload_id=%s", action, confirmation_code, upload_id)
            return {"status": "отправлено в WB", "upload_id": upload_id,
                    "note": "WB обрабатывает цены асинхронно; проверить можно через get_prices через минуту."}
        elif action == "ad_status":
            if not ENABLE_ADS_WRITE:
                return {"error": "Управление рекламой выключено."}
            path = "/adv/v0/start" if payload["action"] == "start" else "/adv/v0/pause"
            await wb_request("advert", "GET", path, params={"id": payload["id"]})
        elif action == "ad_bid":
            if not ENABLE_ADS_WRITE:
                return {"error": "Управление рекламой выключено."}
            await wb_request("advert", "PATCH", "/api/advert/v1/bids", body={"bids": [{
                "advert_id": payload["advert_id"],
                "nm_bids": [{"nm_id": payload["nm_id"], "bid_kopecks": payload["bid_kopecks"],
                             "placement": payload["placement"]}],
            }]})
        elif action == "ad_minus":
            if not ENABLE_ADS_WRITE:
                return {"error": "Управление рекламой выключено."}
            await wb_request("advert", "POST", "/adv/v0/normquery/set-minus", body=payload)
        elif action == "ad_bids":
            if not ENABLE_ADS_WRITE:
                return {"error": "Управление рекламой выключено."}
            await wb_request("advert", "PATCH", "/api/advert/v1/bids", body={"bids": [{
                "advert_id": payload["advert_id"], "nm_bids": payload["nm_bids"]}]})
        elif action == "ad_phrase_bids":
            if not ENABLE_ADS_WRITE:
                return {"error": "Управление рекламой выключено."}
            res = await _apply_phrase_bids(payload)
            audit.info("APPLIED %s code=%s %s", action, confirmation_code, json.dumps(res, ensure_ascii=False))
            return res
        elif action == "ad_phrase_bids_delete":
            if not ENABLE_ADS_WRITE:
                return {"error": "Управление рекламой выключено."}
            await wb_request("advert", "DELETE", "/adv/v0/normquery/bids", body={"bids": [
                {"advert_id": payload["advert_id"], "nm_id": payload["nm_id"], "norm_query": q}
                for q in payload["queries"]]})
        elif action == "ad_items":
            if not ENABLE_ADS_WRITE:
                return {"error": "Управление рекламой выключено."}
            res = await wb_request("advert", "PATCH", "/adv/v0/auction/nms", body={"nms": [{
                "advert_id": payload["advert_id"],
                "nms": {"add": payload["add"], "delete": payload["delete"]}}]})
            audit.info("APPLIED %s code=%s %s", action, confirmation_code, json.dumps(res, ensure_ascii=False))
            return {"status": "выполнено", "ответ WB": res}
        else:
            return {"error": f"Неизвестное действие {action}"}
    except WBError as e:
        audit.info("FAILED %s code=%s %s", action, confirmation_code, e)
        return {"error": str(e)}

    audit.info("APPLIED %s code=%s", action, confirmation_code)
    return {"status": "выполнено", "action": action}


# ---------------------------------------------------------------------------
# HTTP-приложение
# ---------------------------------------------------------------------------


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> JSONResponse:
    _ensure_sync_started()
    return JSONResponse({"status": "ok"})


app = mcp.streamable_http_app(
    # Секрет в пути: без него сервер отвечает 404.
    streamable_http_path=f"/{MCP_SECRET}/mcp",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

# Фоновая загрузка истории стартует вместе с сервером.
_orig_lifespan = getattr(app.router, "lifespan_context", None)
if _orig_lifespan is not None:
    @contextlib.asynccontextmanager
    async def _lifespan_with_sync(a):  # type: ignore[no-untyped-def]
        _ensure_sync_started()
        try:
            async with _orig_lifespan(a) as state:
                yield state
        finally:
            if _sync_task is not None:
                _sync_task.cancel()

    app.router.lifespan_context = _lifespan_with_sync

if __name__ == "__main__":
    log.info("Запуск на порту %s; запись отзывов=%s, цен=%s", PORT, ENABLE_FEEDBACK_WRITE, ENABLE_PRICE_WRITE)
    # access_log выключен, чтобы секретный путь не попадал в логи хостинга.
    uvicorn.run(app, host="0.0.0.0", port=PORT, proxy_headers=True,
                forwarded_allow_ips="*", access_log=False)
