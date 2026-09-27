"""МойСклад (заказ покупателя меняет статус на "доставляется") -> уведомление клиенту
через Wazzup24: сначала в MAX, и если за FALLBACK_TIMEOUT_SEC нет подтверждения
доставки (или пришла явная ошибка) - дублируем в Telegram.

Общая схема:
  1. МойСклад стучится вебхуком на /moysklad/order-webhook при любом изменении
     заказа покупателя (регистрируем вебхук сами при старте, идемпотентно).
  2. Проверяем, что заказ реально ПЕРЕШЁЛ в целевой статус (а не просто в нём
     уже был / изменилось что-то другое) - сравниваем с локальным кэшем
     последнего известного статуса по каждому заказу.
  3. Берём телефон и имя клиента из контрагента (agent) заказа, трек-номер и
     ссылку - из кастомных полей заказа (имена полей задаются в конфиге).
  4. Шлём сообщение через Wazzup24 v3 API в MAX (chatType=max, по телефону).
  5. Ждём вебхук о статусе от Wazzup (подписка messagesAndStatuses). Если за
     FALLBACK_TIMEOUT_SEC не пришло ни одного апдейта по этому messageId, или
     пришла ошибка - дублируем то же сообщение в Telegram (chatType=telegram).

ВАЖНО про статусы Wazzup: официальная документация НЕ перечисляет точные
значения поля "status" в вебхуке о сообщениях (delivered/read/... - неизвестно).
Поэтому логика такая: пришёл любой апдейт без объекта "error" - считаем
доставленным и снимаем с ожидания; пришла ошибка (есть объект "error") -
сразу дублируем в Telegram, не дожидаясь таймаута. Если такая логика окажется
слишком мягкой (например Wazzup шлёт промежуточный статус "sent" ещё до
реальной доставки) - пришли пример реального вебхука, ужесточим до проверки
конкретного значения status.

Настройка (переменные окружения):
  MS_TOKEN                       - Bearer-токен общего JSON API МоегоСклада (см. README:
                                    как получить через POST /security/token)
  MS_TARGET_STATE               - точное название статуса-триггера (по умолчанию "Доставляется")
  MS_TREK_ATTR_ID                - uuid доп. поля заказа с трек-номером (не название! см. README как получить)
  MS_LINK_ATTR_ID                - uuid доп. поля заказа со ссылкой отслеживания
  MS_DELIVERY_COMPANY           - название транспортной компании для текста сообщения (по умолчанию "Деловые Линии")
  MS_PROJECT_FILTER             - слать только если проект заказа равен этому значению (по умолчанию "Cronon")
  MS_EXCLUDE_AGENT_GROUPS       - группы контрагентов через запятую, которым НЕ шлём
                                   (по умолчанию "дилер a - 40%,дилер b,дилер c")
  PUBLIC_URL                    - публичный адрес этого сервиса (для регистрации обоих вебхуков)
  WEBHOOK_SECRET                - произвольная случайная строка, общий секрет для обоих вебхуков
  WAZZUP_TOKEN                  - Bearer-токен Wazzup24 (Настройки -> API)
  WAZZUP_CHANNEL_MAX             - id канала MAX в Wazzup (uuid)
  WAZZUP_CHANNEL_TELEGRAM        - id канала Telegram в Wazzup (uuid)
  FALLBACK_TIMEOUT_SEC           - таймаут ожидания доставки в MAX, сек (по умолчанию 900 = 15 мин)
  STATE_FILE                     - файл кэша статусов заказов (по умолчанию order_state.json)
  PENDING_FILE                   - файл очереди ожидающих подтверждения сообщений (по умолчанию pending.json)

Запуск:
  uvicorn ms_wazzup_notify:app --host 0.0.0.0 --port $PORT
"""
import asyncio
import json
import logging
import os
import re
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("ms-wazzup-notify")

# ---------------------------------------------------------------- настройки
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
MS_TOKEN = os.environ["MS_TOKEN"]
MS_TARGET_STATE = os.getenv("MS_TARGET_STATE", "Доставляется")
MS_TREK_ATTR_ID = os.environ["MS_TREK_ATTR_ID"]  # uuid доп.поля "Трек-номер", см. README
MS_LINK_ATTR_ID = os.environ["MS_LINK_ATTR_ID"]  # uuid доп.поля со ссылкой отслеживания
MS_DELIVERY_COMPANY = os.getenv("MS_DELIVERY_COMPANY", "Деловые Линии")
MS_PROJECT_FILTER = os.getenv("MS_PROJECT_FILTER", "Cronon")  # слать только если проект заказа = это значение
MS_EXCLUDE_AGENT_GROUPS = {
    g.strip().lower() for g in os.getenv(
        "MS_EXCLUDE_AGENT_GROUPS", "дилер a - 40%,дилер b,дилер c"
    ).split(",") if g.strip()
}  # группы контрагентов, которым уведомления НЕ шлём

PUBLIC_URL = os.environ["PUBLIC_URL"].rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET") or secrets.token_hex(16)

WAZZUP_BASE = "https://api.wazzup24.com/v3"
WAZZUP_TOKEN = os.environ["WAZZUP_TOKEN"]
WAZZUP_CHANNEL_MAX = os.environ["WAZZUP_CHANNEL_MAX"]
WAZZUP_CHANNEL_TELEGRAM = os.environ["WAZZUP_CHANNEL_TELEGRAM"]

FALLBACK_TIMEOUT_SEC = int(os.getenv("FALLBACK_TIMEOUT_SEC", "900"))
SWEEP_INTERVAL_SEC = 30

STATE_FILE = Path(os.getenv("STATE_FILE", "order_state.json"))
PENDING_FILE = Path(os.getenv("PENDING_FILE", "pending.json"))

http = httpx.AsyncClient(timeout=20)
MS_HEADERS = {"Authorization": f"Bearer {MS_TOKEN}"}
WAZZUP_HEADERS = {"Authorization": f"Bearer {WAZZUP_TOKEN}"}

MESSAGE_TEMPLATE = (
    "Здравствуйте, {name}! ✨\n"
    "Ваш заказ уже передан в транспортную компанию «{company}» и отправляется к вам.\n"
    "Вы можете легко отслеживать его путь в реальном времени.\n"
    "📦 Трек-номер: {trek}\n"
    "🔗 Ссылка для отслеживания: {link}\n"
    "Спасибо за покупку! Если возникнут вопросы — мы всегда на связи."
)


def norm_phone(num) -> str | None:
    """Только цифры, с кодом страны, без '+' - формат, который просит Wazzup."""
    if not num:
        return None
    d = re.sub(r"\D", "", str(num))
    if len(d) == 11 and d[0] == "8":
        d = "7" + d[1:]
    return d or None


# ---------------------------------------------------------------- МойСклад
async def ms_req(method: str, url_or_path: str, **kw) -> httpx.Response:
    url = url_or_path if url_or_path.startswith("http") else MS_BASE + url_or_path
    r = await http.request(method, url, headers=MS_HEADERS, **kw)
    if r.status_code >= 400:
        log.error("МойСклад %s %s -> %s %s", method, url, r.status_code, r.text[:300])
    r.raise_for_status()
    return r


async def fetch_order(href: str) -> dict:
    return (await ms_req("GET", href, params={"expand": "state,agent,project,agent.group"})).json()


def attr_value(order: dict, attr_id: str) -> str:
    for a in order.get("attributes", []):
        a_id = ((a.get("meta") or {}).get("href") or "").rsplit("/", 1)[-1]
        if a_id == attr_id or a.get("id") == attr_id:
            v = a.get("value")
            if isinstance(v, dict):
                return str(v.get("name") or v.get("value") or "")
            return str(v or "")
    return ""


async def ensure_ms_webhook():
    """Регистрируем вебхук на UPDATE заказов покупателей, если такого ещё нет."""
    url = f"{PUBLIC_URL}/moysklad/order-webhook?token={WEBHOOK_SECRET}"
    existing = (await ms_req("GET", "/entity/webhook")).json().get("rows", [])
    for wh in existing:
        if wh.get("url") == url and wh.get("entityType") == "customerorder":
            log.info("вебхук МоегоСклада уже зарегистрирован (id=%s)", wh["id"])
            return
    r = await ms_req("POST", "/entity/webhook", json={
        "url": url, "action": "UPDATE", "entityType": "customerorder",
    })
    log.info("вебхук МоегоСклада зарегистрирован: %s", r.json().get("id"))


# ---------------------------------------------------------------- Wazzup
async def ensure_wazzup_webhook():
    url = f"{PUBLIC_URL}/wazzup/webhook?token={WEBHOOK_SECRET}"
    await http.request("PATCH", f"{WAZZUP_BASE}/webhooks", headers=WAZZUP_HEADERS, json={
        "webhooksUri": url,
        "subscriptions": {"messagesAndStatuses": True},
    })
    log.info("вебхук Wazzup зарегистрирован на %s", url)


async def wazzup_send(channel_id: str, chat_type: str, phone: str, text: str, crm_message_id: str) -> str | None:
    r = await http.post(f"{WAZZUP_BASE}/message", headers=WAZZUP_HEADERS, json={
        "channelId": channel_id, "chatType": chat_type, "phone": phone,
        "text": text, "crmMessageId": crm_message_id,
    })
    if r.status_code >= 400:
        log.error("Wazzup send (%s) -> %s %s", chat_type, r.status_code, r.text[:300])
        return None
    message_id = r.json().get("messageId")
    log.info("Wazzup: отправлено в %s, messageId=%s, phone=%s", chat_type, message_id, phone)
    return message_id


# ---------------------------------------------------------------- состояние
def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            log.exception("не удалось прочитать %s", path)
    return default


def save_json(path: Path, data):
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        log.exception("не удалось сохранить %s (нужен постоянный диск!)", path)


order_state: dict[str, str] = load_json(STATE_FILE, {})          # order_id -> последний известный статус
pending: dict[str, dict] = load_json(PENDING_FILE, {})           # messageId -> {order_id, phone, text, sent_at}


# ---------------------------------------------------------------- бизнес-логика
async def handle_order_update(href: str):
    order = await fetch_order(href)
    order_id = order["id"]
    state_name = ((order.get("state") or {}).get("name")) or ""
    prev = order_state.get(order_id)
    order_state[order_id] = state_name
    save_json(STATE_FILE, order_state)

    if state_name != MS_TARGET_STATE or prev == MS_TARGET_STATE:
        return  # не переход в целевой статус - ничего не делаем

    project_name = ((order.get("project") or {}).get("name")) or ""
    if project_name != MS_PROJECT_FILTER:
        log.info("заказ %s: проект %r != %r, уведомление не отправляется",
                  order_id, project_name, MS_PROJECT_FILTER)
        return

    agent = order.get("agent") or {}
    group_name = ((agent.get("group") or {}).get("name")) or ""
    if group_name.strip().lower() in MS_EXCLUDE_AGENT_GROUPS:
        log.info("заказ %s: группа контрагента %r в списке исключений, уведомление не отправляется",
                  order_id, group_name)
        return

    phone = norm_phone(agent.get("phone"))
    name = agent.get("name") or "клиент"
    trek = attr_value(order, MS_TREK_ATTR_ID) or "—"
    link = attr_value(order, MS_LINK_ATTR_ID) or "—"
    log.info("заказ %s: доп.поля заказа: %s", order_id,
             [(a.get("id"), a.get("name"), a.get("value")) for a in order.get("attributes", [])])
    log.info("заказ %s: ищу trek_id=%s -> %r, link_id=%s -> %r",
             order_id, MS_TREK_ATTR_ID, trek, MS_LINK_ATTR_ID, link)

    if not phone:
        log.warning("заказ %s: у клиента %s не найден телефон, письмо не отправлено", order_id, name)
        return

    text = MESSAGE_TEMPLATE.format(name=name, company=MS_DELIVERY_COMPANY, trek=trek, link=link)
    message_id = await wazzup_send(WAZZUP_CHANNEL_MAX, "max", phone, text, f"ms-order-{order_id}-max")
    if not message_id:
        # сразу не удалось отправить в MAX - пробуем Telegram без ожидания
        await wazzup_send(WAZZUP_CHANNEL_TELEGRAM, "telegram", phone, text, f"ms-order-{order_id}-tg")
        return

    pending[message_id] = {
        "order_id": order_id, "phone": phone, "text": text,
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }
    save_json(PENDING_FILE, pending)


async def handle_wazzup_status(message_id: str, has_error: bool):
    info = pending.get(message_id)
    if not info:
        return  # не наше сообщение / уже обработано
    if has_error:
        log.warning("Wazzup сообщил об ошибке по messageId=%s, дублирую в Telegram", message_id)
        await wazzup_send(WAZZUP_CHANNEL_TELEGRAM, "telegram", info["phone"], info["text"],
                           f"ms-order-{info['order_id']}-tg")
    else:
        log.info("messageId=%s: получен статус без ошибки, считаю доставленным", message_id)
    del pending[message_id]
    save_json(PENDING_FILE, pending)


async def sweep_pending():
    while True:
        await asyncio.sleep(SWEEP_INTERVAL_SEC)
        now = datetime.now(timezone.utc)
        for message_id, info in list(pending.items()):
            sent_at = datetime.fromisoformat(info["sent_at"])
            if (now - sent_at).total_seconds() >= FALLBACK_TIMEOUT_SEC:
                log.warning("messageId=%s: нет подтверждения %d сек, дублирую в Telegram",
                            message_id, FALLBACK_TIMEOUT_SEC)
                await wazzup_send(WAZZUP_CHANNEL_TELEGRAM, "telegram", info["phone"], info["text"],
                                   f"ms-order-{info['order_id']}-tg")
                del pending[message_id]
                save_json(PENDING_FILE, pending)


# ---------------------------------------------------------------- FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await ensure_ms_webhook()
        await ensure_wazzup_webhook()
    except Exception:
        log.exception("не удалось зарегистрировать вебхуки при старте")
    task = asyncio.create_task(sweep_pending())
    yield
    task.cancel()
    await http.aclose()


app = FastAPI(lifespan=lifespan)


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"ok": True, "pending": len(pending)}


@app.post("/moysklad/order-webhook")
async def moysklad_webhook(request: Request, token: str):
    if token != WEBHOOK_SECRET:
        raise HTTPException(403)
    body = await request.json()
    for ev in body.get("events", []):
        meta = ev.get("meta") or {}
        if meta.get("type") != "customerorder":
            continue
        try:
            await handle_order_update(meta["href"])
        except Exception:
            log.exception("ошибка обработки события заказа %s", meta.get("href"))
    return {"ok": True}


@app.post("/wazzup/webhook")
async def wazzup_webhook(request: Request, token: str):
    if token != WEBHOOK_SECRET:
        raise HTTPException(403)
    body = await request.json()
    if body.get("test"):
        return {"ok": True}  # проверочный запрос при подключении вебхука
    for msg in body.get("messages", []):
        message_id = msg.get("messageId")
        if not message_id:
            continue
        try:
            await handle_wazzup_status(message_id, has_error=bool(msg.get("error")))
        except Exception:
            log.exception("ошибка обработки статуса сообщения %s", message_id)
    return {"ok": True}
