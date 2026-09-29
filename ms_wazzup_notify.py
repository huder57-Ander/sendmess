"""МойСклад (заказ покупателя меняет статус на "доставляется" ИЛИ создаётся заново) ->
уведомление клиенту через Wazzup24: сначала в MAX, и если за FALLBACK_TIMEOUT_SEC нет
подтверждения доставки (или пришла явная ошибка) - дублируем в Telegram.

Два независимых сценария:
  A. Смена статуса на MS_TARGET_STATE ("Доставляется") - как раньше: трек-номер + ссылка.
  B. Создание нового заказа (событие CREATE) - состав заказа, сумма, способ оплаты,
     адрес доставки, ссылка на личный кабинет. Отправляется РОВНО ОДИН РАЗ на заказ
     (учёт в NOTIFIED_NEW_FILE), чтобы повторные правки уже отправленного заказа не
     вызывали повторную отправку - для этого используется именно событие CREATE, а
     не проверка текущего статуса "Новый" (иначе правка старого заказа в статусе
     "Новый" после первого деплоя тоже вызвала бы уведомление).
     Два нюанса сценария B:
       - После CREATE ждём NEW_ORDER_DELAY_SEC (по умолчанию 5 мин), т.к. номер
         заказа (поле "name", например "S-2079") присваивается в МоёмСкладе не
         мгновенно; ожидание идёт в фоне, вебхуку сразу отвечаем 200.
       - Если через это время заказ всё ещё черновик (поле "applicable"=false,
         чекбокс "Проведено" не стоит) - НЕ отправляем и НЕ отмечаем как
         обработанный. Далее каждый UPDATE этого заказа проверяет "applicable"
         заново (maybe_send_new_order_notification) - как только заказ проведут,
         уведомление уйдёт тогда, без дополнительного ожидания.

Общая схема:
  1. МойСклад стучится вебхуком на /moysklad/order-webhook на CREATE и на UPDATE
     заказов покупателей (регистрируем оба вебхука сами при старте, идемпотентно).
  2. Для UPDATE - проверяем, что заказ реально ПЕРЕШЁЛ в целевой статус (сравниваем
     с локальным кэшем последнего известного статуса по каждому заказу).
  3. Оба сценария проверяют общие фильтры: проект заказа (MS_PROJECT_FILTER) и
     теги контрагента (MS_EXCLUDE_AGENT_GROUPS, поле "Группы" в карточке контрагента).
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
  MS_PAYMENT_ATTR_ID             - uuid доп. поля "Способ оплаты" (для уведомления о новом заказе)
  MS_DELIVERY_COMPANY           - название транспортной компании для текста сообщения (по умолчанию "Деловые Линии")
  MS_PROJECT_FILTER             - слать только если проект заказа равен этому значению (по умолчанию "Cronon")
  MS_CONTACT_PHONE               - телефон менеджера для сообщения о новом заказе
  MS_ACCOUNT_URL                  - ссылка на личный кабинет клиента (по умолчанию cronon.ru/client_account/orders)
  NEW_ORDER_DELAY_SEC             - задержка после CREATE перед проверкой/отправкой, сек (по умолчанию 300 = 5 мин)
  MS_EXCLUDE_AGENT_GROUPS       - теги контрагента через запятую, которым НЕ шлём
                                   (это поле "Группы" в карточке контрагента - теги, а НЕ служебный "Отдел")
                                   (по умолчанию "дилер a - 40%,дилер b,дилер c")
  PUBLIC_URL                    - публичный адрес этого сервиса (для регистрации обоих вебхуков)
  WEBHOOK_SECRET                - произвольная случайная строка, общий секрет для обоих вебхуков
  WAZZUP_TOKEN                  - Bearer-токен Wazzup24 (Настройки -> API)
  WAZZUP_CHANNEL_MAX             - id канала MAX в Wazzup (uuid)
  WAZZUP_CHANNEL_TELEGRAM        - id канала Telegram в Wazzup (uuid)
  FALLBACK_TIMEOUT_SEC           - таймаут ожидания доставки в MAX, сек (по умолчанию 900 = 15 мин)
  STATE_FILE                     - файл кэша статусов заказов (по умолчанию order_state.json)
  PENDING_FILE                   - файл очереди ожидающих подтверждения сообщений (по умолчанию pending.json)
  NOTIFIED_NEW_FILE               - файл учёта заказов, о создании которых уже уведомили (по умолчанию notified_new.json)

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
MS_PAYMENT_ATTR_ID = os.getenv("MS_PAYMENT_ATTR_ID", "")  # uuid доп.поля "Способ оплаты" (для уведомления о новом заказе)
MS_DELIVERY_COMPANY = os.getenv("MS_DELIVERY_COMPANY", "Деловые Линии")
MS_PROJECT_FILTER = os.getenv("MS_PROJECT_FILTER", "Cronon")  # слать только если проект заказа = это значение
MS_CONTACT_PHONE = os.getenv("MS_CONTACT_PHONE", "+7 (977) 760 06 30")
MS_ACCOUNT_URL = os.getenv("MS_ACCOUNT_URL", "https://cronon.ru/client_account/orders")
NEW_ORDER_DELAY_SEC = int(os.getenv("NEW_ORDER_DELAY_SEC", "300"))  # ждём 5 мин после CREATE - номер заказа присваивается не сразу
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

NEW_ORDER_TEMPLATE = (
    "Здравствуйте, {name}! Ваш заказ №{order_number} принят. Спасибо, что выбрали Cronon 🤍\n\n"
    "📌 Состав заказа:\n{positions}\n\n"
    "Сумма к оплате: {sum} руб. ({payment_method})\n\n"
    "📦 Адрес доставки: {address} (пожалуйста, проверьте правильность адреса)\n\n"
    "Менеджер свяжется с вами, чтобы согласовать сроки и процесс доставки.\n\n"
    "📍 Детали и статус заказа в личном кабинете: {account_url}\n\n"
    "📞 Не хотите ждать звонка или нужно срочно внести правки? Позвоните нам: {contact_phone}"
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
    return (await ms_req("GET", href, params={"expand": "state,agent,project"})).json()


async def fetch_order_positions_text(order_href: str) -> str:
    """-> "● Название — 2 шт.\n● Другое название — 1 шт." по позициям заказа."""
    data = (await ms_req("GET", f"{order_href}/positions",
                           params={"expand": "assortment", "limit": 100})).json()
    lines = []
    for row in data.get("rows", []):
        title = ((row.get("assortment") or {}).get("name")) or "—"
        qty = row.get("quantity") or 0
        qty_str = str(int(qty)) if qty == int(qty) else str(qty)
        lines.append(f"● {title} — {qty_str} шт.")
    return "\n".join(lines) or "—"


def attr_value(order: dict, attr_id: str) -> str:
    for a in order.get("attributes", []):
        a_id = ((a.get("meta") or {}).get("href") or "").rsplit("/", 1)[-1]
        if a_id == attr_id or a.get("id") == attr_id:
            v = a.get("value")
            if isinstance(v, dict):
                return str(v.get("name") or v.get("value") or "")
            return str(v or "")
    return ""


async def ensure_ms_webhook(action: str):
    """Регистрируем вебхук на заданное действие (UPDATE/CREATE) заказов покупателей, если такого ещё нет."""
    url = f"{PUBLIC_URL}/moysklad/order-webhook?token={WEBHOOK_SECRET}"
    existing = (await ms_req("GET", "/entity/webhook")).json().get("rows", [])
    for wh in existing:
        if wh.get("url") == url and wh.get("entityType") == "customerorder" and wh.get("action") == action:
            log.info("вебхук МоегоСклада (%s) уже зарегистрирован (id=%s)", action, wh["id"])
            return
    r = await ms_req("POST", "/entity/webhook", json={
        "url": url, "action": action, "entityType": "customerorder",
    })
    log.info("вебхук МоегоСклада (%s) зарегистрирован: %s", action, r.json().get("id"))


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
pending: dict[str, dict] = load_json(PENDING_FILE, {})           # messageId -> {order_id, phone, text, crm_suffix, sent_at}
NOTIFIED_NEW_FILE = Path(os.getenv("NOTIFIED_NEW_FILE", "notified_new.json"))
notified_new_orders: set[str] = set(load_json(NOTIFIED_NEW_FILE, []))  # order_id заказов, о создании которых уже уведомили


# ---------------------------------------------------------------- бизнес-логика
def passes_filters(order: dict, order_id: str) -> tuple[bool, dict]:
    """Проверка фильтров проект/теги. Возвращает (прошёл?, agent)."""
    project_name = ((order.get("project") or {}).get("name")) or ""
    if project_name != MS_PROJECT_FILTER:
        log.info("заказ %s: проект %r != %r, уведомление не отправляется",
                  order_id, project_name, MS_PROJECT_FILTER)
        return False, {}

    agent = order.get("agent") or {}
    agent_tags = {t.strip().lower() for t in (agent.get("tags") or [])}
    log.info("заказ %s: контрагент=%r, теги=%r", order_id, agent.get("name"), agent.get("tags"))
    if agent_tags & MS_EXCLUDE_AGENT_GROUPS:
        log.info("заказ %s: у контрагента есть тег из списка исключений (%s), уведомление не отправляется",
                  order_id, agent_tags & MS_EXCLUDE_AGENT_GROUPS)
        return False, {}
    return True, agent


async def send_with_fallback(order_id: str, phone: str, text: str, crm_suffix: str):
    """Шлём в MAX; если сразу не вышло - тут же дублируем в Telegram; если вышло -
    кладём в pending, дальше судьбу решает handle_wazzup_status/sweep_pending."""
    message_id = await wazzup_send(WAZZUP_CHANNEL_MAX, "max", phone, text, f"ms-order-{order_id}{crm_suffix}-max")
    if not message_id:
        await wazzup_send(WAZZUP_CHANNEL_TELEGRAM, "telegram", phone, text, f"ms-order-{order_id}{crm_suffix}-tg")
        return
    pending[message_id] = {
        "order_id": order_id, "phone": phone, "text": text, "crm_suffix": crm_suffix,
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }
    save_json(PENDING_FILE, pending)


async def handle_order_update(href: str):
    order = await fetch_order(href)
    order_id = order["id"]
    state_name = ((order.get("state") or {}).get("name")) or ""
    prev = order_state.get(order_id)
    order_state[order_id] = state_name
    save_json(STATE_FILE, order_state)

    # Заказ мог быть создан как черновик (см. maybe_send_new_order_notification) -
    # проверяем на каждом UPDATE, не наступил ли момент "Проведено", раз CREATE его пропустил.
    await maybe_send_new_order_notification(order, href)

    if state_name != MS_TARGET_STATE or prev == MS_TARGET_STATE:
        return  # не переход в целевой статус - ничего не делаем

    ok, agent = passes_filters(order, order_id)
    if not ok:
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
    await send_with_fallback(order_id, phone, text, "")


async def maybe_send_new_order_notification(order: dict, href: str):
    """Уведомление о новом заказе - шлём один раз на заказ, и только когда он
    реально "Проведён" (applicable=true). Если заказ ещё черновик - НЕ помечаем
    как обработанный, чтобы поймать момент проведения позже через UPDATE."""
    order_id = order["id"]
    if order_id in notified_new_orders:
        return

    if not order.get("applicable"):
        log.info("заказ %s: черновик (не проведён), жду проведения", order_id)
        return

    notified_new_orders.add(order_id)
    save_json(NOTIFIED_NEW_FILE, sorted(notified_new_orders))

    ok, agent = passes_filters(order, order_id)
    if not ok:
        return

    phone = norm_phone(agent.get("phone"))
    name = agent.get("name") or "клиент"
    if not phone:
        log.warning("заказ %s: у клиента %s не найден телефон, уведомление о заказе не отправлено", order_id, name)
        return

    positions = await fetch_order_positions_text(href)
    sum_rub = (order.get("sum") or 0) / 100
    sum_str = format(sum_rub, ",.0f").replace(",", " ")
    payment_method = attr_value(order, MS_PAYMENT_ATTR_ID) or "—" if MS_PAYMENT_ATTR_ID else "—"
    address = order.get("shipmentAddress") or "—"

    text = NEW_ORDER_TEMPLATE.format(
        name=name, order_number=order.get("name") or order_id, positions=positions,
        sum=sum_str, payment_method=payment_method, address=address,
        account_url=MS_ACCOUNT_URL, contact_phone=MS_CONTACT_PHONE,
    )
    await send_with_fallback(order_id, phone, text, "-new")


background_tasks: set[asyncio.Task] = set()  # держим ссылки, чтобы задачи не собрал GC


async def handle_new_order_create_event(href: str):
    """По событию CREATE ждём NEW_ORDER_DELAY_SEC (номер заказа в МоёмСкладе
    присваивается не мгновенно), затем проверяем заказ. Не await'сится в самом
    вебхуке, иначе МойСклад решит, что сервис не отвечает, и будет ретраить."""
    await asyncio.sleep(NEW_ORDER_DELAY_SEC)
    try:
        order = await fetch_order(href)
        await maybe_send_new_order_notification(order, href)
    except Exception:
        log.exception("ошибка отложенной обработки нового заказа %s", href)


async def handle_wazzup_status(message_id: str, has_error: bool):
    info = pending.get(message_id)
    if not info:
        return  # не наше сообщение / уже обработано
    if has_error:
        log.warning("Wazzup сообщил об ошибке по messageId=%s, дублирую в Telegram", message_id)
        await wazzup_send(WAZZUP_CHANNEL_TELEGRAM, "telegram", info["phone"], info["text"],
                           f"ms-order-{info['order_id']}{info.get('crm_suffix', '')}-tg")
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
                                   f"ms-order-{info['order_id']}{info.get('crm_suffix', '')}-tg")
                del pending[message_id]
                save_json(PENDING_FILE, pending)


# ---------------------------------------------------------------- FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await ensure_ms_webhook("UPDATE")
        await ensure_ms_webhook("CREATE")
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
        action = ev.get("action")
        if action == "CREATE":
            task = asyncio.create_task(handle_new_order_create_event(meta["href"]))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
            continue
        try:
            await handle_order_update(meta["href"])
        except Exception:
            log.exception("ошибка обработки события заказа (%s) %s", action, meta.get("href"))
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
