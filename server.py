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
) -> Any:
    url = HOSTS[service] + path
    key = f"{method} {url} {json.dumps(params, sort_keys=True)} {json.dumps(body, sort_keys=True)}"
    if cache and key in _cache and time.time() - _cache[key][0] < CACHE_TTL:
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

    data = resp.json() if resp.content else None
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
    result = {}
    for name, host in HOSTS.items():
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(host + "/ping", headers={"Authorization": WB_TOKEN})
            result[name] = "OK" if r.status_code == 200 else f"ошибка {r.status_code}"
        except Exception as e:  # noqa: BLE001
            result[name] = f"нет связи: {e}"
    result["запись_отзывы"] = "включена" if ENABLE_FEEDBACK_WRITE else "выключена"
    result["запись_цены"] = "включена" if ENABLE_PRICE_WRITE else "выключена"
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
    """Текущие остатки на складах WB по артикулам и складам."""
    rows = await wb_request(
        "statistics", "GET", "/api/v1/supplier/stocks",
        params={"dateFrom": "2019-06-20"}, cache=True,
    ) or []
    by_article: dict[Any, dict] = defaultdict(lambda: {"quantity": 0, "inWayToClient": 0, "inWayFromClient": 0, "warehouses": {}})
    for r in rows:
        k = (r.get("nmId"), r.get("supplierArticle"), r.get("techSize"))
        a = by_article[k]
        a["quantity"] += int(r.get("quantity") or 0)
        a["inWayToClient"] += int(r.get("inWayToClient") or 0)
        a["inWayFromClient"] += int(r.get("inWayFromClient") or 0)
        wh = r.get("warehouseName")
        a["warehouses"][wh] = a["warehouses"].get(wh, 0) + int(r.get("quantity") or 0)
    return {
        "items": [
            {"nmId": k[0], "supplierArticle": k[1], "size": k[2], **v}
            for k, v in by_article.items()
        ]
    }


@mcp.tool()
async def get_finance_report(date_from: str, date_to: str, details: bool = False) -> dict:
    """Отчёт о реализации (финансы) за период, даты YYYY-MM-DD.
    Сводка: продажи, комиссия, логистика, хранение, штрафы, к перечислению."""
    rows: list[dict] = []
    rrdid = 0
    for _ in range(20):  # до 20 страниц по 100 000 строк
        page = await wb_request(
            "statistics", "GET", "/api/v5/supplier/reportDetailByPeriod",
            params={"dateFrom": date_from, "dateTo": date_to, "limit": 100000, "rrdid": rrdid},
            cache=True,
        )
        if not page:
            break
        rows.extend(page)
        rrdid = page[-1].get("rrd_id", 0)
        if len(page) < 100000:
            break

    def s(field: str, where=lambda r: True) -> float:
        return round(sum(float(r.get(field) or 0) for r in rows if where(r)), 2)

    out = {
        "period": f"{date_from} — {date_to}",
        "rows": len(rows),
        "retail_amount_sales": s("retail_amount", lambda r: r.get("doc_type_name") == "Продажа"),
        "retail_amount_returns": s("retail_amount", lambda r: r.get("doc_type_name") == "Возврат"),
        "for_pay_sales": s("ppvz_for_pay", lambda r: r.get("doc_type_name") == "Продажа"),
        "for_pay_returns": s("ppvz_for_pay", lambda r: r.get("doc_type_name") == "Возврат"),
        "delivery_rub": s("delivery_rub"),
        "storage_fee": s("storage_fee"),
        "penalty": s("penalty"),
        "deduction": s("deduction"),
        "acceptance": s("acceptance"),
    }
    out["approx_to_transfer"] = round(
        out["for_pay_sales"] - out["for_pay_returns"] - out["delivery_rub"]
        - out["storage_fee"] - out["penalty"] - out["deduction"] - out["acceptance"], 2
    )
    out["note"] = "approx_to_transfer — оценка; точная сумма в еженедельном отчёте WB."
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
