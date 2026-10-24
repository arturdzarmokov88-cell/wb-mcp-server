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
import base64
import hashlib
import hmac
import json
import logging
import os
import sys
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

CONFIRM_TTL_SECONDS = 600

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

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.request(
            method,
            url,
            params={k: v for k, v in (params or {}).items() if v is not None},
            json=body,
            headers={"Authorization": WB_TOKEN},
        )

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
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(host + "/ping", headers={"Authorization": WB_TOKEN})
            if r.status_code == 200:
                return name, "OK"
            if r.status_code == 401:
                return name, "нет доступа (в токене не отмечен этот раздел)"
            return name, f"ошибка {r.status_code}"
        except Exception as e:  # noqa: BLE001
            return name, f"нет связи: {e}"

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


@mcp.tool()
async def get_stocks() -> dict:
    """Текущие остатки на складах WB по артикулам и размерам, с разбивкой по складам
    и товарами в пути. Отчёт формируется на стороне WB, это занимает до минуты;
    результат кешируется на 10 минут."""
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


@mcp.tool()
async def get_cards(limit: int = 100) -> dict:
    """Карточки товаров: артикулы, названия, бренд, предмет (до limit карточек)."""
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
    return {"cards": cards[:limit]}


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


@mcp.tool()
async def get_ad_stats(date_from: str, date_to: str, campaign_ids: list[int] | None = None) -> dict:
    """Статистика рекламы за период (YYYY-MM-DD, не больше 31 дня): показы, клики, CTR, CPC,
    расход, корзины, заказы, ДРР — по кампаниям и по товарам. Без campaign_ids берутся
    активные и приостановленные кампании."""
    d1 = datetime.strptime(date_from, "%Y-%m-%d")
    d2 = datetime.strptime(date_to, "%Y-%m-%d")
    if (d2 - d1).days > 30 or d2 < d1:
        return {"error": "Период должен быть от 1 до 31 дня."}

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
# Запись: двухшаговое подтверждение
# ---------------------------------------------------------------------------

_SIGN_KEY = hashlib.sha256(b"confirm:" + MCP_SECRET.encode()).digest()
# Уже применённые коды (в пределах одного экземпляра сервера).
_used_codes: dict[str, float] = {}


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _sign(body: str) -> str:
    return _b64(hmac.new(_SIGN_KEY, body.encode(), hashlib.sha256).digest()[:16])


def _make_code(action: str, payload: dict) -> str:
    body = _b64(json.dumps(
        {"a": action, "p": payload, "e": int(time.time()) + CONFIRM_TTL_SECONDS},
        ensure_ascii=False, separators=(",", ":"),
    ).encode())
    return f"{body}.{_sign(body)}"


def _read_code(code: str) -> dict | None:
    try:
        body, sig = code.strip().split(".", 1)
    except ValueError:
        return None
    if not hmac.compare_digest(sig, _sign(body)):
        return None
    try:
        data = json.loads(_unb64(body))
    except Exception:  # noqa: BLE001
        return None
    if data.get("e", 0) < time.time():
        return None
    return data


def _new_confirmation(action: str, payload: dict, preview: dict) -> dict:
    code = _make_code(action, payload)
    audit.info("PREPARED %s sig=%s %s", action, code[-8:], json.dumps(payload, ensure_ascii=False))
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
    return _new_confirmation(
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
    return _new_confirmation(
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
    return _new_confirmation(
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
    return _new_confirmation(
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
    return _new_confirmation(
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
    return _new_confirmation(
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


@mcp.tool()
async def apply_change(confirmation_code: str) -> dict:
    """Шаг 2: применить подготовленное изменение. Вызывать ТОЛЬКО после явного согласия пользователя."""
    item = _read_code(confirmation_code)
    if not item:
        return {"error": "Код неверный или истёк. Подготовьте изменение заново."}
    now = time.time()
    for k in [k for k, exp in _used_codes.items() if exp < now]:
        del _used_codes[k]
    if confirmation_code in _used_codes:
        return {"error": "Этот код уже использован. Подготовьте изменение заново."}
    _used_codes[confirmation_code] = item["e"]
    confirmation_code = confirmation_code[-8:]  # для журнала

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
    return JSONResponse({"status": "ok"})


app = mcp.streamable_http_app(
    # Секрет в пути: без него сервер отвечает 404.
    streamable_http_path=f"/{MCP_SECRET}/mcp",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

if __name__ == "__main__":
    log.info("Запуск на порту %s; запись отзывов=%s, цен=%s", PORT, ENABLE_FEEDBACK_WRITE, ENABLE_PRICE_WRITE)
    # access_log выключен, чтобы секретный путь не попадал в логи хостинга.
    uvicorn.run(app, host="0.0.0.0", port=PORT, proxy_headers=True,
                forwarded_allow_ips="*", access_log=False)
