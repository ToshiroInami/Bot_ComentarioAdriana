#!/usr/bin/env python3
# bot_optimizado_fixed.py - versión parcheada con aislamiento de sesiones y robustez
# PATCHED: persistencia de STRING_SESSION en disco, detección de sesiones no autorizadas
# y guardado periódico de session para sobrevivir reinicios en entornos en la nube.

import os, re, json, random, asyncio, unicodedata, traceback
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Set, List
from dotenv import load_dotenv
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from logging import getLogger, StreamHandler, Formatter, INFO
from logging.handlers import RotatingFileHandler
from filelock import FileLock

load_dotenv()

# ------------------ nueva excepción ------------------
class UnauthorizedSession(Exception):
    """Raised when a session (StringSession or file) is not authorized and should not be retried."""
    pass

# ------------------ helpers ------------------

def sg(k: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(k, default)
    return v.strip() if isinstance(v, str) else v

# --- CONFIG: valores solicitados (y nuevos para seguridad/persistencia) ---
DEBUG_MODE = (sg("DEBUG_MODE") or "false").lower() in ("1","true","yes")
LOG_FILE = sg("LOG_FILE") or "bot_optimizado.log"
ALLOW_SEND = (sg("ALLOW_SEND") or "true").lower() in ("1","true","yes")
ALLOW_FORWARD = (sg("ALLOW_FORWARD") or "true").lower() in ("1","true","yes")
HANDLE = (sg("HANDLE") or "Yukochipro").strip().lstrip("@")
AT_HANDLE = f"@{HANDLE}"
APPEND_HANDLE_TO_FORWARDS = (sg("APPEND_HANDLE_TO_FORWARDS") or "false").lower() in ("1","true","yes")
FORWARD_ONLY_MEDIA = (sg("FORWARD_ONLY_MEDIA") or "true").lower() in ("1","true","yes")

# Palabras, mensajes, logs
PALABRAS_CLAVE = [k.strip() for k in (sg("PALABRAS_CLAVE") or "Arbol genealogico,titularidad,reniec,acta de nacimiento,seguidores,dox,ayuda,info,quien").split(",") if k.strip()]
MSG_GRUPO = sg("MSG_GRUPO") or f"👋 {{mention}}, bienvenido al grupo, Tengo lo que buscas. Escríbeme por privado: {AT_HANDLE}"
MSG_KEYWORD = sg("MSG_KEYWORD") or f"📌 {{mention}}, tengo justo lo que buscas. Escríbeme: {AT_HANDLE}"
MSG_PRIVADO = sg("MSG_PRIVADO") or f"oyee vi tu mensaje en el grupo, que estas buscando?"
MSG_AUTO = sg("MSG_AUTO") or f"Escríbeme a mi cuenta principal: {AT_HANDLE} - aquí estoy activo"
MSG_FORWARD_FOOTER = sg("MSG_FORWARD_FOOTER") or f"Más info: {AT_HANDLE}"
SPAMMER_GROUP = sg("SPAMMER_GROUP")
LOGS_CHANNEL = sg("LOGS_CHANNEL")
LOGS_CHANNEL_ENVIO = sg("LOGS_CHANNEL_ENVIO")

# targets/excludes
EXPLICIT_TARGET_CHAT_IDS = set()
if sg("EXPLICIT_TARGET_CHAT_IDS"):
    for x in sg("EXPLICIT_TARGET_CHAT_IDS").split(","):
        try: EXPLICIT_TARGET_CHAT_IDS.add(int(x.strip()))
        except Exception: pass
EXPLICIT_TARGET_FILE = sg("EXPLICIT_TARGET_FILE")
if EXPLICIT_TARGET_FILE and os.path.exists(EXPLICIT_TARGET_FILE):
    try:
        data = json.load(open(EXPLICIT_TARGET_FILE, 'r', encoding='utf-8'))
        for i in data: EXPLICIT_TARGET_CHAT_IDS.add(int(i))
    except Exception: pass
EXCLUDE_TARGET_GROUPS: List[str] = [g.strip().lower() for g in (sg("EXCLUDE_TARGET_GROUPS") or "publicidad de spam").split(",") if g.strip()]
EXCLUDE_TARGET_IDS: Set[int] = set()
try:
    for x in (sg("EXCLUDE_TARGET_IDS") or "").split(","):
        xs = x.strip()
        if xs: EXCLUDE_TARGET_IDS.add(int(xs))
except Exception:
    EXCLUDE_TARGET_IDS = set()

# timing & limits
STAGGER_STEP_SECONDS = float(sg("STAGGER_STEP_SECONDS") or 30.0)
STAGGER_RANDOM_JITTER = float(sg("STAGGER_RANDOM_JITTER") or 15.0)
FORWARD_LAST_N = int(sg("FORWARD_LAST_N") or 5)
FORWARDS_PER_ROUND = int(sg("FORWARDS_PER_ROUND") or FORWARD_LAST_N)
PER_PUB_DELAY_MIN = float(sg("PER_PUB_DELAY_MIN") or 50.0)
PER_PUB_DELAY_MAX = float(sg("PER_PUB_DELAY_MAX") or 80.0)
POST_ROUND_DELAY_MIN = float(sg("POST_ROUND_DELAY_MIN") or 50.0)
POST_ROUND_DELAY_MAX = float(sg("POST_ROUND_DELAY_MAX") or 80.0)

RESEND_COOLDOWN_SECONDS = int(sg("RESEND_COOLDOWN_SECONDS") or 220)
CHAT_DAILY_LIMIT = int(sg("CHAT_DAILY_LIMIT") or 10000)
MAX_PUBS_PER_CHAT = int(sg("MAX_PUBS_PER_CHAT") or 5)
GLOBAL_MAX_CONCURRENT_FORWARDS = int(sg("GLOBAL_MAX_CONCURRENT_FORWARDS") or 8)
PER_ACCOUNT_MIN_INTERVAL = float(sg("PER_ACCOUNT_MIN_INTERVAL") or 80.0)

# Nuevo: intervalo mínimo específico para operaciones de forwarding.
# Por defecto 0 -> los forwards respetan sólo PER_PUB_DELAY_MIN/MAX.
FORWARD_ACCOUNT_MIN_INTERVAL = float(sg("FORWARD_ACCOUNT_MIN_INTERVAL") or 0.0)

# Nuevo: cooldown por usuario para detección de keywords en grupos (segundos)
KEYWORD_USER_COOLDOWN_SECONDS = int(sg("KEYWORD_USER_COOLDOWN_SECONDS") or 120)

# Replies
WELCOME_DELAY_MIN = float(sg("WELCOME_DELAY_MIN") or 3.0)
WELCOME_DELAY_MAX = float(sg("WELCOME_DELAY_MAX") or 11.0)
KEYWORD_REPLY_DELAY_MIN = float(sg("KEYWORD_REPLY_DELAY_MIN") or 3.0)
KEYWORD_REPLY_DELAY_MAX = float(sg("KEYWORD_REPLY_DELAY_MAX") or 10.0)
PRIVATE_REPLY_DELAY_MIN = float(sg("PRIVATE_REPLY_DELAY_MIN") or 6.0)
PRIVATE_REPLY_DELAY_MAX = float(sg("PRIVATE_REPLY_DELAY_MAX") or 10.0)
FAST_REPLY_SECONDS = float(sg("FAST_REPLY_SECONDS") or 1.0)
FAST_REPLY_WINDOW_SECONDS = int(sg("FAST_REPLY_WINDOW_SECONDS") or 60*60*24)
AUTO_REPLY_COOLDOWN_SECONDS = int(sg("AUTO_REPLY_COOLDOWN_SECONDS") or 300)

# Simultaneous responders
WELCOME_SIMULTANEOUS = int(sg("WELCOME_SIMULTANEOUS") or 2)
KEYWORD_SIMULTANEOUS = int(sg("KEYWORD_SIMULTANEOUS") or 2)
PRIVATE_SIMULTANEOUS = int(sg("PRIVATE_SIMULTANEOUS") or 1)

# coordinación compartida
GLOBAL_SHARED_FILE = sg("GLOBAL_SHARED_FILE") or "global_responses.json"
GLOBAL_SHARED_LOCK = GLOBAL_SHARED_FILE + ".lock"
SHARED_WINDOW_KEYWORD = int(sg("SHARED_WINDOW_KEYWORD") or 120)
SHARED_WINDOW_WELCOME = int(sg("SHARED_WINDOW_WELCOME") or 600)
SHARED_WINDOW_PRIVATE = int(sg("SHARED_WINDOW_PRIVATE") or 600)

# Nuevas variables para manejo de Flood/pausas/guardado
MAX_FLOOD_MULT = float(sg("MAX_FLOOD_MULT") or 3.0)
BASE_PAUSE_EXTRA_SEC = int(sg("BASE_PAUSE_EXTRA_SEC") or 10)
SAVE_STATE_INTERVAL = int(sg("SAVE_STATE_INTERVAL") or 120)
KEEPALIVE_INTERVAL = int(sg("KEEPALIVE_INTERVAL") or 1200)
MIN_PAUSE_ON_FWD = int(sg("MIN_PAUSE_ON_FWD") or 5)

# logger
logger = getLogger("bot_optimizado")
logger.setLevel(INFO)
fmt = Formatter(f'[%(asctime)s] %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
sh = StreamHandler(); sh.setFormatter(fmt); logger.addHandler(sh)
rfh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding='utf-8')
rfh.setFormatter(fmt); logger.addHandler(rfh)
logger.info(f"FLAGS: ALLOW_SEND={ALLOW_SEND} ALLOW_FORWARD={ALLOW_FORWARD} FORWARD_ONLY_MEDIA={FORWARD_ONLY_MEDIA} APPEND_HANDLE_TO_FORWARDS={APPEND_HANDLE_TO_FORWARDS}")

class SimpleTelegramLogger:
    def __init__(self, client: TelegramClient):
        self.client = client; self._recent: Dict[str, datetime] = {}; self.throttle_seconds = int(sg("TELEGRAM_LOG_THROTTLE") or 10)
    async def send(self, channel: Optional[str], text: str, force: bool = False):
        if not self.client or not channel: return
        now = datetime.now(); key = text.strip(); last = self._recent.get(key)
        if last and (now - last).total_seconds() < self.throttle_seconds and not force: return
        try:
            if force or channel in (LOGS_CHANNEL, LOGS_CHANNEL_ENVIO):
                await self.client.send_message(channel, text)
            else:
                if ALLOW_SEND:
                    await self.client.send_message(channel, text)
                else:
                    logger.info(f"SIMLOG -> {channel}: {text[:120]}")
            self._recent[key] = now
        except Exception as e:
            logger.debug(f"TeleLogger fallo al enviar a {channel}: {e}")

# shared file helpers
def _read_shared() -> Dict[str, List[str]]:
    try:
        if not os.path.exists(GLOBAL_SHARED_FILE): return {}
        with FileLock(GLOBAL_SHARED_LOCK):
            with open(GLOBAL_SHARED_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
        out: Dict[str, List[str]] = {}
        for k, v in raw.items():
            if isinstance(v, list): out[k] = [x for x in v if isinstance(x, str)]
            elif isinstance(v, str): out[k] = [v]
            else: out[k] = []
        return out
    except Exception:
        return {}

def _write_shared(d: Dict[str, List[str]]):
    try:
        cutoff = datetime.now() - timedelta(days=1); clean = {}
        for k, vals in d.items():
            kept = []
            for iso in vals:
                try:
                    t = datetime.fromisoformat(iso)
                    if t > cutoff: kept.append(iso)
                except Exception:
                    continue
            if kept: clean[k] = kept
        with FileLock(GLOBAL_SHARED_LOCK):
            with open(GLOBAL_SHARED_FILE, "w", encoding="utf-8") as f:
                json.dump(clean, f)
    except Exception:
        try:
            with open(GLOBAL_SHARED_FILE, "w", encoding="utf-8") as f: json.dump(d, f)
        except Exception:
            pass

def has_recent_shared_response(key: str, window_seconds: int, max_allowed: int = 1) -> bool:
    try:
        data = _read_shared(); vals = data.get(key, [])
        if not vals: return False
        now = datetime.now(); count = 0
        for iso in vals:
            try:
                t = datetime.fromisoformat(iso)
                if (now - t).total_seconds() < window_seconds: count += 1
            except Exception:
                continue
        return count >= max_allowed
    except Exception:
        return False

def register_shared_response(key: str):
    try:
        data = _read_shared(); lst = data.get(key, []); lst.append(datetime.now().isoformat()); data[key] = lst; _write_shared(data)
    except Exception:
        pass

# text helpers
def _normalize(text: Optional[str]) -> str:
    if not text: return ""
    t = unicodedata.normalize("NFKD", text)
    return t.encode("ascii", "ignore").decode("ascii").lower()

def _limpiar_menciones_y_links(text: Optional[str]) -> str:
    if not text: return ""
    t = re.sub(r'@\w{3,32}', ' ', text)
    t = re.sub(r'tg://user\?id=\d+', ' ', t)
    t = re.sub(r'(https?://)?t\.me/\S+', ' ', t)
    t = re.sub(r'https?://\S+', ' ', t)
    return t

def _tiene_keywords_match(texto: str) -> Optional[str]:
    if not texto: return None
    limpio = _normalize(_limpiar_menciones_y_links(texto))
    if not limpio: return None
    limpio = re.sub(r'\s+', ' ', limpio)
    for kw in PALABRAS_CLAVE:
        kwn = _normalize(kw)
        if not kwn: continue
        try:
            if re.search(rf'\b{re.escape(kwn)}\b', limpio): return kwn
        except re.error:
            if kwn in limpio: return kwn
    return None

def mention_md(username: Optional[str], uid: int, first_name: Optional[str] = None) -> str:
    if username: label = f"@{username}"
    else: label = first_name or "amigo"
    label = str(label).replace("[", "").replace("]", "")
    return f"[{label}](tg://user?id={uid})"

def _ensure_handle_in_message(msg: str) -> str:
    if not msg: return msg
    if AT_HANDLE.lower() in msg.lower(): return msg
    return f"{msg} \n\n{AT_HANDLE}"

def _label_for_user_obj(obj, uid_fallback: Optional[int] = None) -> str:
    try:
        if obj is None:
            return f"{uid_fallback}" if uid_fallback is not None else "unknown"
        uname = getattr(obj, 'username', None)
        if uname:
            return f"@{uname}"
        fn = getattr(obj, 'first_name', None)
        if fn:
            return fn
        return str(getattr(obj, 'id', uid_fallback or 'unknown'))
    except Exception:
        return str(uid_fallback or "unknown")

# sessions/utils
ENV_FILE = os.getenv("ENV_FILE", ".env")
MAX_ACCOUNTS = int(sg("MAX_ACCOUNTS") or 20)
SESSION_PREFIX = os.getenv("SESSION_PREFIX", "bot.session")
SESSIONS_DIR = os.getenv("SESSIONS_DIR", "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)

def find_accounts(max_acc=MAX_ACCOUNTS) -> List[Dict]:
    accounts = []
    for i in range(1, max_acc+1):
        if i == 1:
            api_id = os.getenv("API_ID"); api_hash = os.getenv("API_HASH"); phone = os.getenv("PHONENUMBER")
        else:
            api_id = os.getenv(f"API_ID{i}"); api_hash = os.getenv(f"API_HASH{i}"); phone = os.getenv(f"PHONENUMBER{i}")
        if api_id and api_hash and phone:
            try: api_id_int = int(api_id)
            except Exception: api_id_int = None
            accounts.append({"idx": i, "api_id": api_id_int, "api_hash": api_hash.strip(), "phone": phone.strip()})
    return accounts

GLOBAL_FORWARD_SEMAPHORE = asyncio.Semaphore(GLOBAL_MAX_CONCURRENT_FORWARDS)

class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = float(min_interval)
        self._last: Optional[datetime] = None
    async def wait(self):
        if not self._last:
            self._last = datetime.now(); return
        elapsed = (datetime.now() - self._last).total_seconds()
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        self._last = datetime.now()

class DialogCache:
    def __init__(self, client, ttl_seconds=300):
        self.client = client
        self.ttl = ttl_seconds
        self._cache_at = datetime.min
        self._value = None
    async def get(self):
        if (datetime.now() - self._cache_at).total_seconds() < self.ttl and self._value is not None:
            return self._value
        dialogs = [d async for d in self.client.iter_dialogs(limit=500)]
        self._value = dialogs
        self._cache_at = datetime.now()
        return self._value

async def iniciar_usuario(api_id: int, api_hash: str, phone: str, nombre: str, known_account_ids: Set[int], idx: int = 1):
    if not api_id or not api_hash or not phone:
        logger.warning(f"[{nombre}] Credenciales incompletas, saltando.")
        return
    digits = re.sub(r'[^0-9]', '', phone)

    # prepare file session path to allow deletion on invalidation
    session_path = os.path.join(SESSIONS_DIR, f"{SESSION_PREFIX}-{digits}.session")
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    # ---------- nueva lógica: persistir STRING_SESSION en archivo dentro de SESSIONS_DIR ----------
    string_session_file = os.path.join(SESSIONS_DIR, f"STRING_SESSION{idx}.string")

    # asegurar variable definida para evitar NameError en finally
    session_save_task = None

    session_env = None
    if os.path.exists(string_session_file):
        try:
            with open(string_session_file, 'r', encoding='utf-8') as f:
                t = f.read().strip()
                if t:
                    session_env = t
        except Exception:
            session_env = None

    if not session_env:
        env_val = os.getenv(f"STRING_SESSION{idx}")
        if env_val:
            session_env = env_val.strip()
            try:
                tmp = string_session_file + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f: f.write(session_env)
                os.replace(tmp, string_session_file)
                logger.info(f"[{nombre}] Persistida STRING_SESSION{idx} en {string_session_file}")
            except Exception:
                logger.debug(f"[{nombre}] No se pudo persistir STRING_SESSION{idx} en archivo")

    if session_env:
        client = TelegramClient(StringSession(session_env), api_id, api_hash)
        using_string_session = True
    else:
        client = TelegramClient(session_path, api_id, api_hash)
        using_string_session = False

    tlg_logger = SimpleTelegramLogger(client)

    # tareas/estado locales
    paused_until = None; paused_forwarding_until = None
    usuarios_no_contactable: Dict[int, datetime] = {}
    usuarios_auto_respondidos: Dict[int, datetime] = {}
    usuarios_esperando_respuesta: Dict[int, datetime] = {}
    group_response_cooldown: Dict[tuple, datetime] = {}
    ultimos_nuevos_por_chat: Dict[int, Dict] = {}
    flood_count = 0; flood_window_start = datetime.now()

    paused_file = f"paused_{digits}.json"; last_forwarded_file = f"last_forwarded_{digits}.json"; sent_counts_file = f"sent_counts_{digits}.json"

    # estado persistente cargado/guardado
    def load_paused():
        nonlocal paused_until, paused_forwarding_until
        try:
            if os.path.exists(paused_file):
                with open(paused_file, 'r', encoding='utf-8') as f:
                    j = json.load(f)
                if j.get('paused_until'):
                    dt = datetime.fromisoformat(j['paused_until']);
                    if dt > datetime.now(): paused_until = dt
                if j.get('paused_forwarding_until'):
                    dt = datetime.fromisoformat(j['paused_forwarding_until']);
                    if dt > datetime.now(): paused_forwarding_until = dt
        except Exception:
            pass
    def save_paused():
        try:
            out = {}
            if paused_until: out['paused_until'] = paused_until.isoformat()
            if paused_forwarding_until: out['paused_forwarding_until'] = paused_forwarding_until.isoformat()
            tmp = paused_file + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f: json.dump(out, f, ensure_ascii=False)
            os.replace(tmp, paused_file)
        except Exception:
            pass

    def load_last_forwarded():
        try:
            if os.path.exists(last_forwarded_file):
                with open(last_forwarded_file, 'r', encoding='utf-8') as f: raw = json.load(f)
                out = {}
                for k, v in raw.items():
                    try:
                        chat_s, msg_s = k.split(":"); out[(int(chat_s), int(msg_s))] = datetime.fromisoformat(v)
                    except Exception:
                        continue
                return out
        except Exception:
            pass
        return {}

    def save_last_forwarded(d):
        try:
            out = {f"{k[0]}:{k[1]}": v.isoformat() for k, v in d.items()}
            tmp = last_forwarded_file + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f: json.dump(out, f, ensure_ascii=False)
            os.replace(tmp, last_forwarded_file)
        except Exception:
            pass

    def load_sent_counts():
        try:
            if os.path.exists(sent_counts_file):
                with open(sent_counts_file, 'r', encoding='utf-8') as f: raw = json.load(f)
                if raw.get('date') == date.today().isoformat(): return raw.get('counts', {})
        except Exception:
            pass
        return {}

    def save_sent_counts(counts):
        try:
            tmp = sent_counts_file + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f: json.dump({'date': date.today().isoformat(), 'counts': counts}, f, ensure_ascii=False)
            os.replace(tmp, sent_counts_file)
        except Exception:
            pass

    # autosave periódico
    async def autosave_state_loop():
        nonlocal last_forwarded, sent_counts
        try:
            while True:
                try:
                    save_last_forwarded(last_forwarded)
                    save_sent_counts(sent_counts)
                except Exception:
                    logger.debug(f"[{nombre}] autosave fallo: {traceback.format_exc()}")
                await asyncio.sleep(SAVE_STATE_INTERVAL)
        except asyncio.CancelledError:
            return

    # periodic session saver (persistir client.session.save() en archivo)
    async def periodic_session_save(interval=300):
        try:
            while True:
                try:
                    try:
                        s = client.session.save()
                    except Exception:
                        s = None
                    if s:
                        try:
                            tmpf = string_session_file + ".tmp"
                            with open(tmpf, 'w', encoding='utf-8') as f:
                                f.write(s)
                            os.replace(tmpf, string_session_file)
                            logger.debug(f"[{nombre}] session persistida ({len(s)} chars)")
                        except Exception:
                            logger.debug(f"[{nombre}] no se pudo persistir session a file")
                except Exception as e:
                    logger.debug(f"[{nombre}] periodic_session_save error: {e}")
                await asyncio.sleep(max(120, SAVE_STATE_INTERVAL))
        except asyncio.CancelledError:
            return

    # keepalive para detectar sesión invalidada
    async def keepalive_check():
        await asyncio.sleep(random.uniform(0, STAGGER_RANDOM_JITTER + 5))
        try:
            while True:
                try:
                    await client.get_me()
                except Exception as e:
                    m = str(e).lower()
                    logger.debug(f"[{nombre}] Keepalive resultado: {m}")
                    if "auth_key_unregistered" in m or "unauthorized" in m or "not authorized" in m or ("session" in m and "invalid" in m):
                        logger.warning(f"[{nombre}] Keepalive detectó problema de sesión: {e}")
                        try:
                            await tlg_logger.send(LOGS_CHANNEL, f"{nombre} • Sesión invalidada/expirada: {e}", force=True)
                        except Exception:
                            pass
                        # eliminar archivo de sesión local para evitar reusar sesión corrupta
                        if not using_string_session:
                            try:
                                if os.path.exists(session_path):
                                    os.remove(session_path)
                                    logger.info(f"[{nombre}] Archivo de session eliminado: {session_path}")
                            except Exception:
                                logger.debug(f"[{nombre}] No se pudo eliminar session file: {traceback.format_exc()}")
                        # eliminar string persistida también
                        try:
                            if os.path.exists(string_session_file):
                                os.remove(string_session_file)
                                logger.info(f"[{nombre}] Archivo STRING_SESSION removido: {string_session_file}")
                        except Exception:
                            pass

                        try:
                            await client.disconnect()
                        except Exception:
                            pass

                        # lanzar excepción para que el runner lo gestione (no reintentar infinito)
                        raise UnauthorizedSession(f"Keepalive: sesión invalidada para {nombre}: {e}")
                    else:
                        logger.debug(f"[{nombre}] Keepalive error no crítico: {e}")
                sleep_for = KEEPALIVE_INTERVAL * random.uniform(0.8, 1.2)
                await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            return

    # Conectar
    await client.connect()

    # Si la cuenta no está autorizada -> lanzar UnauthorizedSession para que el runner deje de intentar
    if not await client.is_user_authorized():
        logger.warning(f"[{nombre}] Sesión no autorizada, revisa STRING_SESSION{idx} o inicia interactivamente.")
        try: await client.disconnect()
        except: pass
        # borrar string persistida si existe para forzar regeneración manual
        try:
            if os.path.exists(string_session_file):
                os.remove(string_session_file)
                logger.info(f"[{nombre}] Archivo STRING_SESSION removido (no autorizado): {string_session_file}")
        except Exception:
            pass
        raise UnauthorizedSession(f"Session invalid or revoked for {nombre} (idx={idx})")

    # iniciar tarea que persiste la session periódicamente (si tenemos path)
    try:
        if string_session_file:
            session_save_task = asyncio.create_task(periodic_session_save())
    except Exception:
        session_save_task = None

    # Estado inicial
    me = await client.get_me(); me_id = getattr(me, 'id', None); known_account_ids.add(me_id)
    logger.info(f"[{nombre}] conectado ({getattr(me,'username',getattr(me,'first_name',me_id))})")
    m = re.search(r'\d+', nombre); account_idx = int(m.group()) - 1 if m else idx-1
    if STAGGER_STEP_SECONDS == 0:
        account_start_offset = 0.0
    else:
        account_start_offset = account_idx * STAGGER_STEP_SECONDS + random.uniform(0, STAGGER_RANDOM_JITTER)
    logger.info(f"[{nombre}] start_offset={account_start_offset:.1f}s")

    load_paused(); last_forwarded = load_last_forwarded(); sent_counts = load_sent_counts(); recent_send_buffer = []

    def is_paused(): return paused_until is not None and datetime.now() < paused_until
    def is_forwarding_paused(): return paused_forwarding_until is not None and datetime.now() < paused_forwarding_until

    # Manejo robusto de FloodWait y errores
    async def handle_floodwait(wait_seconds: int, reason: str = "", forward_action: bool = False):
        nonlocal paused_until, paused_forwarding_until, flood_count, flood_window_start
        now = datetime.now()
        if (now - flood_window_start).total_seconds() > (30 * 60):
            flood_window_start = now; flood_count = 0
        flood_count += 1

        extra = max(BASE_PAUSE_EXTRA_SEC, int(wait_seconds * 0.15))
        mult = 1.0 + min(flood_count * 0.3, MAX_FLOOD_MULT - 1.0)
        jitter = int(extra * random.uniform(0, 0.3))
        pause_for = max(int(wait_seconds * mult) + extra + jitter, wait_seconds + MIN_PAUSE_ON_FWD)
        until = now + timedelta(seconds=pause_for)
        if forward_action:
            paused_forwarding_until = until
        else:
            paused_until = until
        save_paused()
        logger.warning(f"[{nombre}] Pausa por límite activada hasta {until} (motivo={reason}, wait={wait_seconds}s, mult={mult:.2f})")
        try:
            account_label = f"@{getattr(me,'username')}" if getattr(me,'username',None) else (getattr(me,'first_name', nombre) or nombre)
            await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • Pausa por límite activada hasta {until} (motivo={reason})", force=True)
        except Exception:
            pass

    # bienvenida
    async def bienvenida_por_join_local(event):
        try:
            if not getattr(event, 'is_group', False): return
            if not (getattr(event, 'user_joined', False) or getattr(event, 'user_added', False)): return
            if is_paused(): return
            user = await event.get_user();
            if not user: return
            uid = user.id; username = getattr(user, 'username', None); chat_id = getattr(event, 'chat_id', None)
            ultimos_nuevos_por_chat[chat_id] = {'uid': uid, 'username': username, 'ts': datetime.now()}
            welcome_key = f"welcome:{chat_id}_user:{uid}"
            if has_recent_shared_response(welcome_key, SHARED_WINDOW_WELCOME, WELCOME_SIMULTANEOUS): return
            await asyncio.sleep(random.uniform(WELCOME_DELAY_MIN, WELCOME_DELAY_MAX))
            if has_recent_shared_response(welcome_key, SHARED_WINDOW_WELCOME, WELCOME_SIMULTANEOUS): return
            register_shared_response(welcome_key)
            mention = mention_md(username, uid, getattr(user, 'first_name', None))
            texto = _ensure_handle_in_message(MSG_GRUPO.format(mention=mention))
            if ALLOW_SEND:
                try:
                    await event.reply(texto, parse_mode='md')
                except errors.FloodWaitError as e:
                    await handle_floodwait(e.seconds, 'bienvenida', forward_action=False)
                except Exception as e:
                    logger.debug(f"[{nombre}] Error enviar bienvenida: {e}")
            else:
                logger.info(f"SIM -> Bienvenida a {uid} en chat {chat_id}: {texto}")
            logger.info(f"[{nombre}] Bienvenida enviada a {username or getattr(user,'first_name',uid)}")
            try:
                account_label = f"@{getattr(me,'username')}" if getattr(me,'username',None) else (getattr(me,'first_name', nombre) or nombre)
                recipient_label = _label_for_user_obj(user, uid)
                await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • Bienvenida a {recipient_label} en chat id={chat_id}", force=True)
            except Exception: pass
        except errors.FloodWaitError as e:
            await handle_floodwait(e.seconds, 'bienvenida_top', forward_action=False)
        except Exception as e:
            logger.debug(f"[{nombre}] Error bienvenida: {e}")

    # Handler unificado
    async def new_message_unificado(event):
        try:
            is_private = getattr(event, 'is_private', False)
            is_group = getattr(event, 'is_group', False)
            if not (is_group or is_private): return

            txt = ''
            if getattr(event, 'raw_text', None): txt = (event.raw_text or '').strip()
            elif getattr(event.message, 'message', None) is not None: txt = (event.message.message or '').strip()
            if not txt: return

            sender = await event.get_sender()
            if not sender: return
            uid = sender.id
            username = getattr(sender, 'username', None)

            account_label = f"@{getattr(me,'username')}" if getattr(me,'username',None) else (getattr(me,'first_name', nombre) or nombre)
            sender_label = _label_for_user_obj(sender, uid)

            if is_private:
                ltxt = txt.lower()
                if ltxt in ('/status', 'status'):
                    paused_info = {}
                    if os.path.exists(paused_file):
                        try:
                            with open(paused_file, 'r', encoding='utf-8') as f: paused_info = json.load(f)
                        except Exception: paused_info = {}
                    last_count = len(load_last_forwarded().keys())
                    texto = (f"🛠 Estado {nombre}:\n- paused: {bool(paused_until)}\n"
                             f"- paused_until: {paused_info.get('paused_until')}\n"
                             f"- paused_forwarding_until: {paused_info.get('paused_forwarding_until')}\n"
                             f"- last_forwarded_entries: {last_count}\n")
                    try:
                        if ALLOW_SEND: await event.reply(texto)
                        else: logger.info(f"SIM -> status reply to {uid}: {texto}")
                    except Exception: pass
                    return

                last_auto = usuarios_auto_respondidos.get(uid)
                if last_auto and (datetime.now() - last_auto).total_seconds() < AUTO_REPLY_COOLDOWN_SECONDS:
                    return

                try:
                    await asyncio.sleep(FAST_REPLY_SECONDS)
                    if ALLOW_SEND: await event.reply(_ensure_handle_in_message(MSG_AUTO))
                    else: logger.info(f"SIM -> auto reply to {uid}: {MSG_AUTO}")
                    usuarios_auto_respondidos[uid] = datetime.now()
                    usuarios_esperando_respuesta.pop(uid, None)
                    logger.info(f"[{nombre}] Auto-respuesta enviada a {uid}")
                    try:
                        await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • Auto-respuesta enviada a {sender_label}", force=True)
                    except Exception: pass
                except errors.FloodWaitError as e:
                    await handle_floodwait(e.seconds, 'auto_reply_private', forward_action=True)
                except Exception as e:
                    logger.debug(f"[{nombre}] Error auto-reply: {e}")
                return

            # GROUP logic
            if is_group:
                if is_paused(): return
                if getattr(event.message, 'service', False): return
                if len(txt.splitlines()) > 3 or len(txt.split()) > 30: return
                if uid == me_id or uid in known_account_ids: return

                info_nuevo = ultimos_nuevos_por_chat.get(getattr(event, 'chat_id', None))
                if info_nuevo:
                    ts = info_nuevo.get('ts')
                    if ts and (datetime.now() - ts).total_seconds() <= 120:
                        username_nuevo = info_nuevo.get('username')
                        if username_nuevo and ('hola' in txt.lower() or 'bienven' in txt.lower()): return

                matched = _tiene_keywords_match(txt)
                if not matched: return

                # --- nuevo control: cooldown por usuario para keywords ---
                user_key = f"user_kw:{uid}"
                if has_recent_shared_response(user_key, KEYWORD_USER_COOLDOWN_SECONDS, 1):
                    logger.debug(f"[{nombre}] Ignorando keyword de usuario {uid} por cooldown user_key")
                    return

                key = (uid, getattr(event, 'chat_id', None))
                last = group_response_cooldown.get(key)
                if last and (datetime.now() - last).total_seconds() < 30: return

                group_key = f"group:{getattr(event,'chat_id',None)}_user:{uid}_kw"
                if not has_recent_shared_response(group_key, SHARED_WINDOW_KEYWORD, KEYWORD_SIMULTANEOUS):
                    await asyncio.sleep(random.uniform(KEYWORD_REPLY_DELAY_MIN, KEYWORD_REPLY_DELAY_MAX))
                    if not has_recent_shared_response(group_key, SHARED_WINDOW_KEYWORD, KEYWORD_SIMULTANEOUS):
                        register_shared_response(group_key)
                        mention = mention_md(username, uid, getattr(sender, 'first_name', None))
                        texto = _ensure_handle_in_message(MSG_KEYWORD.format(mention=mention))
                        try:
                            if ALLOW_SEND: await event.reply(texto, parse_mode='md')
                            else: logger.info(f"SIM -> Reply keyword in {getattr(event,'chat_id',None)}: {texto}")
                            logger.info(f"[{nombre}] Respondió keyword '{matched}' en chat {getattr(event,'chat_id',None)} por {sender_label}")
                            try:
                                await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • Keyword '{matched}' detectada por {sender_label} en chat id={getattr(event,'chat_id',None)}", force=True)
                            except Exception: pass
                        except errors.FloodWaitError as e:
                            await handle_floodwait(e.seconds, 'keyword_reply', forward_action=False)
                        except Exception as e:
                            logger.debug(f"[{nombre}] Error replying keyword: {e}")

                        # registrar cooldown por usuario para evitar respuestas repetidas al mismo usuario
                        register_shared_response(user_key)

                        group_response_cooldown[key] = datetime.now()

                # privados coordinados
                await asyncio.sleep(random.uniform(PRIVATE_REPLY_DELAY_MIN, PRIVATE_REPLY_DELAY_MAX))
                if uid in usuarios_no_contactable and datetime.now() < usuarios_no_contactable[uid]: return
                try:
                    private_key = f"private:user:{uid}"
                    if has_recent_shared_response(private_key, SHARED_WINDOW_PRIVATE, PRIVATE_SIMULTANEOUS):
                        logger.debug(f"Otra cuenta ya envió privado a {uid} (límite alcanzado)")
                    else:
                        register_shared_response(private_key)
                        usuarios_esperando_respuesta[uid] = datetime.now()
                        if ALLOW_SEND:
                            await client.send_message(uid, _ensure_handle_in_message(MSG_PRIVADO))
                        else:
                            logger.info(f"SIM -> DM to {uid}: {MSG_PRIVADO}")
                        logger.info(f"[{nombre}] Privado enviado a {uid}")
                        try:
                            await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • Privado enviado a {sender_label}", force=True)
                        except Exception: pass
                except errors.FloodWaitError as e:
                    await handle_floodwait(e.seconds, 'privado_after_kw', forward_action=True)
                except Exception as e:
                    m = str(e).lower()
                    if any(x in m for x in ("userisblocked", "chatwriteforbidden", "unauthorized", "bot was blocked", "peer not found")):
                        usuarios_no_contactable[uid] = datetime.now() + timedelta(hours=24)
                        logger.warning(f"[{nombre}] No contactable {uid}, marcado 24h")
                        try:
                            await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • No contactable {sender_label} (marcado 24h)", force=True)
                        except Exception: pass
                    else:
                        logger.debug(f"[{nombre}] Error al enviar privado: {e}")

        except Exception as e:
            logger.exception(f"[{nombre}] new_message_unificado: {e}")

    # enviar publicaciones (FORWARD)
    async def enviar_publicaciones_local(account_start_offset: float = 0.0):
        nonlocal paused_until, paused_forwarding_until, last_forwarded, sent_counts, recent_send_buffer
        await asyncio.sleep(account_start_offset)
        if not SPAMMER_GROUP:
            logger.warning(f"[{nombre}] SPAMMER_GROUP no configurado. Saltando forwards.")
            return
        try:
            spam_id = int(SPAMMER_GROUP)
        except Exception:
            logger.error(f"[{nombre}] SPAMMER_GROUP invalido."); return

        try:
            spam_entity = await client.get_entity(spam_id)
        except Exception as e:
            logger.warning(f"[{nombre}] No se pudo obtener entidad del spam_id {spam_id}: {e} — usar id bruto")
            spam_entity = spam_id

        if not ALLOW_FORWARD:
            logger.info(f"[{nombre}] Forwarding deshabilitado (ALLOW_FORWARD=false)."); return

        dialog_cache = DialogCache(client, ttl_seconds=300)
        # *** Usar rate limiter específico para forwards para no forzar PER_ACCOUNT_MIN_INTERVAL ***
        forward_rate_limiter = RateLimiter(FORWARD_ACCOUNT_MIN_INTERVAL)

        empty_rounds = 0; forwarded_today_count = 0; forwarded_window_start = datetime.now()

        while True:
            try:
                # reset diario
                if (datetime.now() - forwarded_window_start).total_seconds() > 24*3600:
                    forwarded_window_start = datetime.now(); forwarded_today_count = 0; sent_counts = {}; save_sent_counts(sent_counts)

                if is_forwarding_paused() or is_paused():
                    await asyncio.sleep(random.uniform(10, 20)); continue

                publicaciones = []
                async for msg in client.iter_messages(spam_id, limit=FORWARD_LAST_N):
                    try:
                        if getattr(msg, 'service', False):
                            continue
                        text_field = getattr(msg, 'message', None)
                        has_media = getattr(msg, 'media', None) is not None
                        caption = getattr(msg, 'caption', None)
                        if FORWARD_ONLY_MEDIA and not has_media:
                            continue
                        if (not text_field or str(text_field).strip() == '') and not has_media and not caption:
                            continue
                        publicaciones.append(msg)
                    except Exception:
                        continue

                logger.info(f"[{nombre}] DEBUG: spam_id={spam_id} publicaciones_raw={len(publicaciones)}")
                try:
                    info_list = [{"id": m.id, "date": getattr(m,'date',None), "has_media": getattr(m,'media',None) is not None, "text_len": len((getattr(m,'message','') or '') or (getattr(m,'caption','') or ''))} for m in publicaciones]
                    logger.debug(f"[{nombre}] DEBUG publicaciones detalles: {info_list}")
                except Exception as e:
                    logger.debug(f"[{nombre}] ERROR al loggear publicaciones: {e}")

                if not publicaciones:
                    await asyncio.sleep(random.uniform(POST_ROUND_DELAY_MIN, POST_ROUND_DELAY_MAX))
                    continue

                pubs_sorted = sorted([{"id": m.id, "date": getattr(m, 'date', datetime.min), "msg": m} for m in publicaciones], key=lambda x: x['date'])
                last_pubs = pubs_sorted[-FORWARD_LAST_N:] if pubs_sorted else []
                final_ids = [p['id'] for p in last_pubs]

                dialogs = await dialog_cache.get()
                chat_map = {}
                for d in dialogs:
                    chat = d.entity
                    if getattr(chat, 'broadcast', False): continue
                    if not getattr(d, 'is_group', False) and not getattr(chat, 'megagroup', False) and not getattr(chat, 'gigagroup', False):
                        continue
                    chat_title = getattr(chat, 'title', None) or ''
                    chat_id = getattr(chat, 'id', None)
                    if chat_id is None or chat_id == spam_id or chat_id in EXCLUDE_TARGET_IDS: continue
                    low_title = chat_title.lower()
                    if any(pat and pat in low_title for pat in EXCLUDE_TARGET_GROUPS): continue
                    if EXPLICIT_TARGET_CHAT_IDS and chat_id not in EXPLICIT_TARGET_CHAT_IDS: continue
                    sent_today = int(sent_counts.get(str(chat_id), 0))
                    chat_map[chat_id] = {'title': chat_title, 'sent_today': sent_today, 'sent_round': 0}

                target_chat_ids = list(chat_map.keys()); random.shuffle(target_chat_ids)
                forwarded_for_round = 0; now = datetime.now()

                account_label = f"@{getattr(me,'username')}" if getattr(me,'username',None) else (getattr(me,'first_name', nombre) or nombre)

                for cid in target_chat_ids:
                    if forwarded_for_round >= FORWARDS_PER_ROUND: break
                    cm = chat_map.get(cid)
                    if not cm: continue
                    if cm['sent_today'] >= CHAT_DAILY_LIMIT: continue
                    if cm['sent_round'] >= MAX_PUBS_PER_CHAT: continue

                    skip = False
                    for mid in final_ids:
                        last = last_forwarded.get((cid, mid))
                        if last and (now - last).total_seconds() <= RESEND_COOLDOWN_SECONDS:
                            skip = True; break
                    if skip: continue

                    try:
                        # <-- aquí usamos forward_rate_limiter en lugar de PER_ACCOUNT_MIN_INTERVAL -->
                        await forward_rate_limiter.wait()
                        async with GLOBAL_FORWARD_SEMAPHORE:
                            if ALLOW_FORWARD:
                                try:
                                    # intentamos batch primero
                                    await client.forward_messages(cid, final_ids, spam_entity)
                                except errors.FloodWaitError as e:
                                    # manejar flood (más conservador)
                                    await handle_floodwait(e.seconds, f"reenviar batch -> {cid}", forward_action=True)
                                    # intentar esperar y luego enviar per-message con delays
                                    for mid in final_ids:
                                        try:
                                            await asyncio.sleep(random.uniform(PER_PUB_DELAY_MIN, PER_PUB_DELAY_MAX))
                                            await client.forward_messages(cid, mid, spam_entity)
                                        except errors.FloodWaitError as e2:
                                            await handle_floodwait(e2.seconds, f"reenviar per-msg -> {cid}", forward_action=True)
                                            break
                                        except Exception as e_mid:
                                            logger.warning(f"[{nombre}] fallo forward msg {mid} -> {cid}: {e_mid}")
                                except Exception as e_batch:
                                    # intentar per-message si falla el batch
                                    logger.debug(f"[{nombre}] batch forward falló a {cid}: {e_batch} — intentando per-message")
                                    for mid in final_ids:
                                        try:
                                            await client.forward_messages(cid, mid, spam_entity)
                                            await asyncio.sleep(random.uniform(PER_PUB_DELAY_MIN, PER_PUB_DELAY_MAX))
                                        except Exception as e_mid:
                                            logger.warning(f"[{nombre}] fallo forward msg {mid} -> {cid}: {e_mid}")
                            else:
                                logger.info(f"SIM -> forward {final_ids} to {cid} ({cm['title']})")

                        ts_now = datetime.now()
                        for mid in final_ids:
                            last_forwarded[(cid, mid)] = ts_now
                        cm['sent_round'] += 1; cm['sent_today'] += 1; sent_counts[str(cid)] = cm['sent_today']
                        forwarded_for_round += 1

                        # Envío minimalista a LOGS_CHANNEL_ENVIO
                        if LOGS_CHANNEL_ENVIO:
                            try:
                                ids_str = ",".join(str(x) for x in final_ids)
                                forward_msg = f"{account_label} envió publicaciones [{ids_str}] a '{cm['title']}' (id={cid})"
                                await tlg_logger.send(LOGS_CHANNEL_ENVIO, forward_msg, force=True)
                            except Exception:
                                logger.debug(f"[{nombre}] No se pudo enviar log de envío a {LOGS_CHANNEL_ENVIO}")

                        if APPEND_HANDLE_TO_FORWARDS and ALLOW_SEND:
                            await asyncio.sleep(random.uniform(0.5, 1.0))
                            footer = MSG_FORWARD_FOOTER
                            try:
                                # <-- usar mismo forward_rate_limiter antes de enviar footer -->
                                await forward_rate_limiter.wait()
                                await client.send_message(cid, footer)
                            except errors.FloodWaitError as f:
                                await handle_floodwait(f.seconds, 'footer_after_forward', forward_action=True)
                            except Exception:
                                logger.debug(f"[{nombre}] No se pudo enviar footer a {cm['title']} ({cid})")

                    except errors.FloodWaitError as f:
                        await handle_floodwait(f.seconds, f"reenviar a '{cm['title']}'", forward_action=True)
                        break
                    except Exception as e:
                        msg = str(e).lower()
                        if "forbidden" in msg or "chatwriteforbidden" in msg:
                            paused_forwarding_until = datetime.now() + timedelta(minutes=20); save_paused()
                            logger.warning(f"[{nombre}] Permisos denegados al reenviar a '{cm['title']}' -> pausa 20m")
                            try:
                                await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • Permisos denegados al reenviar a '{cm['title']}' (id={cid}) -> pausa 20m", force=True)
                            except Exception: pass
                            break
                        else:
                            logger.debug(f"[{nombre}] Error reenviando a '{cm['title']}': {e}")
                            continue

                    await asyncio.sleep(random.uniform(PER_PUB_DELAY_MIN * 0.8, PER_PUB_DELAY_MAX * 1.2))

                # guardado periódico (no esperar al cierre)
                if random.random() < 0.5:
                    save_last_forwarded(last_forwarded); save_sent_counts(sent_counts)

                recent_send_buffer.clear()

                if forwarded_for_round == 0:
                    empty_rounds += 1
                    await asyncio.sleep(random.uniform(POST_ROUND_DELAY_MIN, POST_ROUND_DELAY_MAX))
                else:
                    empty_rounds = 0
                    post_round = random.uniform(POST_ROUND_DELAY_MIN, POST_ROUND_DELAY_MAX)
                    activity_factor = 1.0 + min(forwarded_for_round / max(1, FORWARD_LAST_N), 1.0)
                    await asyncio.sleep(post_round * activity_factor)

            except errors.FloodWaitError as f:
                await handle_floodwait(f.seconds, 'forward_loop', forward_action=True)
            except Exception as e:
                logger.exception(f"[{nombre}] Error en loop de forwarding: {e}")
                await asyncio.sleep(10 + random.uniform(0, 10))

    # registro de handlers y loop
    client.add_event_handler(bienvenida_por_join_local, events.ChatAction())
    client.add_event_handler(new_message_unificado, events.NewMessage(incoming=True))

    # iniciar tareas auxiliares
    autosave_task = asyncio.create_task(autosave_state_loop())
    keepalive_task = asyncio.create_task(keepalive_check())
    runner_forward = asyncio.create_task(enviar_publicaciones_local(account_start_offset=account_start_offset))

    try:
        account_label = f"@{getattr(me,'username')}" if getattr(me,'username',None) else (getattr(me,'first_name', nombre) or nombre)
        await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • STATUS: paused={is_paused()} paused_fwd={is_forwarding_paused()} start_offset={account_start_offset:.1f}s", force=True)
    except Exception: pass

    try:
        await client.run_until_disconnected()
    except Exception as e:
        logger.exception(f"[{nombre}] run_until_disconnected terminó: {e}")
    finally:
        try: autosave_task.cancel()
        except: pass
        try: keepalive_task.cancel()
        except: pass
        try: runner_forward.cancel()
        except: pass
        try:
            if session_save_task:
                session_save_task.cancel()
        except:
            pass
        try: await tlg_logger.send(LOGS_CHANNEL, f"{account_label} • BOT detenido", force=True)
        except: pass
        try: await client.disconnect()
        except: pass

# runner_wrapper mejorado: trata retornos normales como soft-failure y aplica backoff
async def runner_wrapper(api_id, api_hash, phone, nombre, known_account_ids, idx):
    retries = 0
    while True:
        try:
            await iniciar_usuario(api_id, api_hash, phone, nombre, known_account_ids, idx)
            # iniciar_usuario retornó -> aplicar backoff antes de intentar otra vez
            retries += 1
            base = min(2 ** min(retries, 12), 21600)
            jitter = random.uniform(0.5, 1.5)
            wait = int(base * jitter)
            logger.warning(f"[Main] {nombre} terminó. Reintentando en {wait}s (retry={retries})")
            await asyncio.sleep(wait)
        except UnauthorizedSession as ue:
            # Sesión inválida permanente: no reintentar. El operador debe regenerar la session.
            logger.error(f"[Main] {nombre}: sesión inválida/expirada: {ue}. Dejo de reintentar. (Requiere acción manual)")
            return
        except Exception as e:
            retries += 1
            base = min(2 ** min(retries, 12), 21600)
            jitter = random.uniform(0.5, 1.5)
            wait = int(base * jitter)
            logger.exception(f"[Main] Crash en {nombre}: {e}. Reintentando en {wait}s (retry={retries})")
            await asyncio.sleep(wait)

async def main():
    tlg_accounts = find_accounts()
    if not tlg_accounts:
        print("No se encontraron cuentas API_ID/API_HASH/PHONENUMBER en el entorno. Revisa .env.")
        return
    known_account_ids: Set[int] = set(); tareas = []
    for idx, u in enumerate(tlg_accounts, start=1):
        if u['api_id'] and u['api_hash'] and u['phone']:
            nombre = f"Usuario{u['idx']}"
            tareas.append(asyncio.create_task(runner_wrapper(u['api_id'], u['api_hash'], u['phone'], nombre, known_account_ids, u['idx'])))
        else:
            logger.info(f"[Main] Credenciales incompletas para Usuario{u['idx']}, no se iniciará.")
    if not tareas:
        logger.error("[Main] No hay cuentas válidas. Revisa .env."); return

    # IMPORTANTE: return_exceptions=True para que una cuenta que falle no mate a las demás
    results = await asyncio.gather(*tareas, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            logger.warning(f"Una tarea terminó con excepción: {r}")

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Saliendo...")
    except Exception as e: logger.exception(f"Error al iniciar: {e}")
