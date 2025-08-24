# bot_respuestas_contacto.py
# Flujo:
# 1) Bienvenida al unirse (delay humano)
# 2) Detección keyword -> respuesta en grupo (delay humano) -> envío privado (MSG_PRIVADO)
# 3) Si el usuario responde al privado (o si alguien DM directo), el bot responde con MSG_CONTACT
# 4) Respeta cooldown AUTO_REPLY_COOLDOWN (por defecto 5 minutos) para no abusar
# 5) Maneja FloodWait pausando envíos cuando sea necesario

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors
import os
import asyncio
import random
import re
import unicodedata
import html
from datetime import datetime, timedelta

load_dotenv()

# ---------- Config (lee desde .env) ----------
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONENUMBER = os.getenv("PHONENUMBER", "")  # ejemplo: +51945922363
LOGS_CHANNEL = os.getenv("LOGS_CHANNEL")    # canal único de logs (requerido)

# ---------- Mensajes (tus textos) ----------
# Bienvenida en grupo (si el usuario tiene username)
MSG_GRUPO = os.getenv("MSG_GRUPO",
    "👋 Hola @{username}, bienvenido al grupo, escríbeme a @Yukochi, tengo todo lo que necesitas.")
# Bienvenida en grupo (si el usuario no tiene username)
MSG_GRUPO_SIN_USERNAME = os.getenv("MSG_GRUPO_SIN_USERNAME",
    "👋 Hola [{name}](tg://user?id={id}), bienvenido al grupo. Escríbeme a @Yukochi.")
# Mensaje que se responde en el grupo cuando detecta keyword (con username)
MSG_KEYWORD = os.getenv("MSG_KEYWORD",
    "📌 @{username}, tengo justo lo que buscas, escríbeme a @Yukochi.")
# Mensaje que se responde en el grupo cuando detecta keyword (sin username)
MSG_KEYWORD_SIN_USERNAME = os.getenv("MSG_KEYWORD_SIN_USERNAME",
    "📌 [{name}](tg://user?id={id}), tengo justo lo que buscas. Escríbeme a @Yukochi.")
# Mensaje que se envía por privado después de responder en el grupo (proactivo)
MSG_PRIVADO = os.getenv("MSG_PRIVADO", "Holaa vi tu mensaje en el grupo, dime qué buscas?")
# MENSAJE DE CONTACTO (mensaje automático que se envía cuando usuario te escribe en privado
# o cuando responde al privado que le enviaste). ÉSTE es el "mensaje automático" principal.
MSG_CONTACT = os.getenv("MSG_CONTACT", "Sí, esta es mi cuenta principal — comunícate con @Yukochi.")

# ---------- tiempos / cooldowns ----------
AUTO_REPLY_COOLDOWN = int(os.getenv("AUTO_REPLY_COOLDOWN", "300"))   # 5 minutos por defecto (privados)
GROUP_COOLDOWN_SECONDS = int(os.getenv("GROUP_COOLDOWN_SECONDS", "15"))

# Delays configurables (valores por defecto)
WELCOME_DELAY_MIN = float(os.getenv("WELCOME_DELAY_MIN", "2.0"))
WELCOME_DELAY_MAX = float(os.getenv("WELCOME_DELAY_MAX", "5.0"))
GROUP_REPLY_DELAY_MIN = float(os.getenv("GROUP_REPLY_DELAY_MIN", "2.5"))
GROUP_REPLY_DELAY_MAX = float(os.getenv("GROUP_REPLY_DELAY_MAX", "5.5"))
PRIVATE_DELAY_MIN = float(os.getenv("PRIVATE_DELAY_MIN", "1.5"))
PRIVATE_DELAY_MAX = float(os.getenv("PRIVATE_DELAY_MAX", "4.0"))

# Palabras clave (usa tu lista)
PALABRAS_CLAVE = [
    "alguien", "necesito", "dox", "arbol", "quien"
]

# Usuarios excluidos (tu lista)
USUARIOS_EXCLUIDOS = ["Samie 8.0", "Mini Bratz 2.0 ✨", "@Mini Bratz✨", "Kurotomi0002", "Kurotomi0001", "Kurotomi0011", "Kurotomi0012", "Kurotomi0010", "Kurotomi0009", "Quispe012", "Luz Mariana Marulada", "𝓐𝓜𝓔𝓝𝓐𝓩𝓘 | 𝓒𝓐𝓢𝓢", "Verti", "H_U_S_H1", "mencion1_bot", "kurotomi0008", "kurotomi0007", "kurotomi0006", "kurotomi0005", "kurotomi0004", "kurotomi0003", "spam_thor", "..", ".", "Sofia72718", "pablito_by",  "Chu Ngoc Thao", "GHClone1Bot", "Publicidad pilar", "uq0v06od760", "Kratos5545", "Robert Wilson", "makedahbag2", "Leo Melvin", "Romi7726", "Tu Terror", "Spam", "spam", "SpamTerrible", "Joelpayp", "STV1234111", "tupapieldoxer1", "Rogelio Mz", "keychilli2", "Rabbit 2.0", "Samie 5.0", "Bratzz 4.0", "Rabbit2.0", "Samie 6", "SamieSPM", "Samie3.0", "samiessppmm", "Samie 7", "samiessppmm2", "Young Angel 1.0", "Samie...4.0", "Tumejorsitio", "@GroupHelpBot","valeee2008", "Tumejorsitio", "Yensy_zabala", "Tumejorsitiosspam", "terriblespm", "spamterrible3", "ShadowByte00", "multiventas_1", "MoonKatsOF", "Sakoxxxe", "SangMata_beta_bot", "SangMata_BOT", "ServiciosSpam", "SistemasSAC", "Mariomedina420", "capishooo", "capishopp", "ACCESOSPERU_BOT", "GHClone4bot", "Tumejorsitiospam", "SpamBRATVA", "PeruAdmin_bot", "DrWebBot", "Pkmn_games_bot", "RENIECconsultass", "kenichi0x", "Marioj_bot"]  # sin @

# ---------- validaciones iniciales ----------
if not (API_ID and API_HASH and PHONENUMBER and LOGS_CHANNEL):
    raise RuntimeError("Faltan variables de entorno: API_ID, API_HASH, PHONENUMBER, LOGS_CHANNEL")

# ---------- utilidades ----------
def normalize_str(s: str) -> str:
    if not s:
        return ""
    s = str(s).lower().lstrip('@').strip()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

raw_excluded = set()
normalized_excluded = set()
for u in USUARIOS_EXCLUIDOS:
    raw = str(u).lower().lstrip('@').strip()
    raw_excluded.add(raw)
    n = normalize_str(raw)
    if n:
        normalized_excluded.add(n)

def is_excluded_user(user) -> bool:
    if not user:
        return False
    username = getattr(user, "username", None)
    if username:
        u_raw = str(username).lower().lstrip('@').strip()
        if u_raw in raw_excluded:
            return True
        if normalize_str(u_raw) in normalized_excluded:
            return True
    full = ((getattr(user, "first_name", "") or "") + " " + (getattr(user, "last_name", "") or "")).strip().lower()
    if full:
        if full in raw_excluded:
            return True
        if normalize_str(full) in normalized_excluded:
            return True
    return False

def _normalize_for_search(text: str) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", text)
    t = t.encode("ascii", "ignore").decode("ascii")
    return t.lower()

def contains_keyword(text: str) -> bool:
    if not text:
        return False
    cleaned = re.sub(r'@\w{3,32}', ' ', text)
    cleaned = re.sub(r'https?://\S+', ' ', cleaned)
    cleaned = _normalize_for_search(cleaned)
    for kw in PALABRAS_CLAVE:
        k = _normalize_for_search(kw)
        if not k:
            continue
        try:
            if re.search(rf'\b{re.escape(k)}\b', cleaned):
                return True
        except re.error:
            if k in cleaned:
                return True
    return False

# helper para mención clickeable (async)
async def get_mention(client, user_obj):
    try:
        uid = getattr(user_obj, 'id', None) or 0
        username = getattr(user_obj, 'username', None)
        if username:
            username = str(username).lstrip('@').strip()
            mention_html = f"<a href='tg://user?id={uid}'>{html.escape('@' + username)}</a>"
            return f"@{username}", mention_html
        first = getattr(user_obj, 'first_name', '') or ''
        last = getattr(user_obj, 'last_name', '') or ''
        display = ' '.join(filter(None, [first, last])).strip()
        if display:
            nombre_log = f"@{display}"
            mention_html = f"<a href='tg://user?id={uid}'>{html.escape(nombre_log)}</a>"
            return nombre_log, mention_html
        return f"Usuario (id:{uid})", f"<a href='tg://user?id={uid}'>Usuario</a>"
    except Exception:
        return "Usuario", "<a href='tg://user?id=0'>Usuario</a>"

# ---------- Bot ----------
session = "bot_no_forward_usernames.session"
client = TelegramClient(session, API_ID, API_HASH)

# estado global de pausado (por FloodWait)
paused_until = datetime.min
def is_paused() -> bool:
    return datetime.now() < paused_until

# cooldowns y tracking
group_response_cooldown = {}   # key = (user_id, chat_id) -> datetime
usuarios_auto_respondidos = {} # user_id -> datetime (marca cuándo EL BOT YA RESPONDIÓ al usuario con MSG_CONTACT)
# tracking de mensajes proactivos enviados (MSG_PRIVADO) para saber si una respuesta del usuario
# corresponde a haber recibido ese mensaje proactivo antes
last_privado_enviado = {}      # user_id -> datetime

ME_ID = None  # se llenará tras el start()

# ---------- funciones seguras para enviar mensajes (manejando FloodWait) ----------
async def safe_send(entity, text, parse_mode=None):
    global paused_until
    if is_paused():
        return False
    try:
        await client.send_message(entity, text, parse_mode=parse_mode)
        return True
    except errors.FloodWaitError as fe:
        paused_until = datetime.now() + timedelta(seconds=fe.seconds)
        try:
            await client.send_message(LOGS_CHANNEL, f"⚠️ FloodWait detectado: pausando {fe.seconds}s", parse_mode="HTML")
        except Exception:
            pass
        await asyncio.sleep(fe.seconds)
        return False
    except Exception:
        try:
            await client.send_message(LOGS_CHANNEL, f"❌ Error en safe_send (detalle omitido)", parse_mode="HTML")
        except Exception:
            pass
        return False

async def safe_reply_event(event, text, parse_mode=None):
    global paused_until
    if is_paused():
        return False
    try:
        await event.reply(text, parse_mode=parse_mode)
        return True
    except errors.FloodWaitError as fe:
        paused_until = datetime.now() + timedelta(seconds=fe.seconds)
        try:
            await client.send_message(LOGS_CHANNEL, f"⚠️ FloodWait al responder: pausando {fe.seconds}s", parse_mode="HTML")
        except Exception:
            pass
        await asyncio.sleep(fe.seconds)
        return False
    except Exception as e:
        try:
            await client.send_message(LOGS_CHANNEL, f"❌ Error al reply: {html.escape(str(e))}", parse_mode="HTML")
        except Exception:
            pass
        return False

# ---------- Handlers ----------
@client.on(events.ChatAction)
async def on_chat_action(event):
    try:
        if not getattr(event, "is_group", False):
            return

        if getattr(event, "user_joined", False) or getattr(event, "user_added", False):
            users = getattr(event, "users", None)
            if not users and getattr(event, "user_id", None):
                try:
                    users = [await event.get_user()]
                except Exception:
                    users = []

            for u in users or []:
                try:
                    if is_excluded_user(u):
                        try:
                            _, mention_html = await get_mention(client, u)
                            await safe_send(LOGS_CHANNEL, f"⛔ Usuario excluido omitido en bienvenida: {mention_html}", parse_mode="HTML")
                        except Exception:
                            pass
                        continue

                    username = getattr(u, "username", None)
                    uid = getattr(u, "id", None)

                    # Delay aleatorio antes de la bienvenida (simula humano)
                    await asyncio.sleep(random.uniform(WELCOME_DELAY_MIN, WELCOME_DELAY_MAX))

                    if is_paused():
                        await safe_send(LOGS_CHANNEL, f"⏸️ Pausado - omitida bienvenida para {uid} en {getattr(event.chat,'title',event.chat_id)}", parse_mode="HTML")
                        continue

                    # enviar bienvenida en el grupo
                    if username:
                        msg = MSG_GRUPO.format(username=username)
                        await safe_reply_event(event, msg)
                    else:
                        msg = MSG_GRUPO_SIN_USERNAME.format(name=getattr(u, "first_name", "amigo"), id=uid)
                        await safe_reply_event(event, msg, parse_mode="md")

                    # LOG: siempre con mención clickeable (username o nombre)
                    _, mention_html = await get_mention(client, u)
                    chat_title = getattr(event.chat, "title", str(event.chat_id))
                    await safe_send(LOGS_CHANNEL, f"👋 Bienvenida enviada a {mention_html} en <code>{html.escape(chat_title)}</code>", parse_mode="HTML")

                except errors.FloodWaitError as fe:
                    await safe_send(LOGS_CHANNEL, f"⚠️ FloodWait en bienvenida: {fe.seconds}s", parse_mode="HTML")
                    await asyncio.sleep(fe.seconds)
                except Exception as e:
                    await safe_send(LOGS_CHANNEL, f"❌ Error en bienvenida: {html.escape(str(e))}", parse_mode="HTML")
    except Exception:
        return

@client.on(events.NewMessage(incoming=True))
async def on_new_message(event):
    try:
        # ---- Mensajes privados ----
        if getattr(event, "is_private", False):
            try:
                sender = await event.get_sender()
            except Exception:
                sender = None
            if not sender:
                return

            # no responder a uno mismo
            if getattr(sender, "id", None) == ME_ID:
                return

            if getattr(sender, "bot", False) or is_excluded_user(sender):
                return

            uid = getattr(sender, "id", None)
            now = datetime.now()

            # Si el BOT YA RESPONDIÓ con MSG_CONTACT hace menos de AUTO_REPLY_COOLDOWN -> no responder
            last_contact = usuarios_auto_respondidos.get(uid)
            if last_contact and (now - last_contact).total_seconds() < AUTO_REPLY_COOLDOWN:
                return

            # Si existe un MSG_PRIVADO proactivo enviado recientemente (o en el pasado),
            # y el usuario nos escribe (esto indica que responde al mensaje proactivo),
            # respondemos con MSG_CONTACT (mensaje automático), y registramos cooldown.
            # También si no existe MSG_PRIVADO (usuario nos escribe directo), hacemos lo mismo.
            try:
                # Delay pequeño para simular lectura humana (opcional, corto)
                await asyncio.sleep(random.uniform(0.6, 1.6))

                # Responder con el mensaje de contacto (mensaje automático)
                await safe_reply_event(event, MSG_CONTACT)
                usuarios_auto_respondidos[uid] = datetime.now()

                _, mention_html = await get_mention(client, sender)
                await safe_send(LOGS_CHANNEL, f"✉️ Enviado MSG_CONTACT a {mention_html}", parse_mode="HTML")
            except errors.FloodWaitError as fe:
                await safe_send(LOGS_CHANNEL, f"⚠️ FloodWait al auto-responder privado: {fe.seconds}s", parse_mode="HTML")
                await asyncio.sleep(fe.seconds)
            except Exception as e:
                await safe_send(LOGS_CHANNEL, f"❌ Error al auto-responder privado: {html.escape(str(e))}", parse_mode="HTML")
            return  # privado manejado

        # ---- Mensajes en grupos ----
        if not getattr(event, "is_group", False):
            return
        if getattr(event.message, "service", False):
            return

        text = (getattr(event, "raw_text", "") or "").strip()
        if not text:
            return

        try:
            sender = await event.get_sender()
        except Exception:
            sender = None
        if not sender:
            return

        if getattr(sender, "id", None) == ME_ID:
            return

        if getattr(sender, "bot", False) or is_excluded_user(sender):
            return

        # filtros básicos por longitud y caracteres
        if len(text) < 1 or len(text) > 400:
            return
        if text.count("\n") > 3:
            return
        invalid_chars = sum(1 for c in text if not re.match(r"[a-zA-Z0-9\s.,;:?!¡¿()\"'%-áéíóúÁÉÍÓÚñÑ@/]", c))
        if invalid_chars > 6:
            return

        if not contains_keyword(text):
            return

        # Delay aleatorio antes de responder en grupo (simula lectura)
        await asyncio.sleep(random.uniform(GROUP_REPLY_DELAY_MIN, GROUP_REPLY_DELAY_MAX))

        if is_paused():
            chat = await event.get_chat()
            chat_title = getattr(chat, "title", str(event.chat_id))
            _, mention_html = await get_mention(client, sender)
            await safe_send(LOGS_CHANNEL, f"⏸️ Pausado - omitida respuesta en grupo '{html.escape(chat_title)}' para {mention_html}", parse_mode="HTML")
            return

        chat = await event.get_chat()
        chat_title = getattr(chat, "title", str(event.chat_id))

        uid = getattr(sender, "id", None)
        username = getattr(sender, "username", None)
        _, mention_html = await get_mention(client, sender)

        key = (uid, event.chat_id)
        now = datetime.now()
        last = group_response_cooldown.get(key)
        if last and (now - last).total_seconds() < GROUP_COOLDOWN_SECONDS:
            return

        # RESPUESTA en grupo
        try:
            if username:
                await safe_reply_event(event, MSG_KEYWORD.format(username=username))
            else:
                await safe_reply_event(event, MSG_KEYWORD_SIN_USERNAME.format(name=getattr(sender, "first_name", "amigo"), id=uid), parse_mode="md")

            # LOG con mención HTML
            await safe_send(
                LOGS_CHANNEL,
                (f"<b>🔎 PALABRA CLAVE DETECTADA</b>\n"
                 f"<b>👥 Grupo:</b> <code>{html.escape(chat_title)}</code>\n"
                 f"<b>🙋 Usuario:</b> {mention_html}\n"
                 f"<b>💬 Mensaje:</b>\n<code>{html.escape(text)}</code>"),
                parse_mode="HTML"
            )
            group_response_cooldown[key] = datetime.now()
        except Exception as e:
            await safe_send(LOGS_CHANNEL, f"❌ Error al responder en grupo: {html.escape(str(e))}", parse_mode="HTML")
            return

        # Espera aleatoria y luego envío privado proactivo (MSG_PRIVADO).
        await asyncio.sleep(random.uniform(PRIVATE_DELAY_MIN, PRIVATE_DELAY_MAX))

        # Si estamos en pausa (FloodWait) no enviamos el privado
        if is_paused():
            await safe_send(LOGS_CHANNEL, f"⏸️ Pausado - omitido envío privado posterior a respuesta de grupo para {mention_html}", parse_mode="HTML")
            return

        try:
            sent = await safe_send(uid, MSG_PRIVADO)
            if sent:
                # Guardamos que enviamos este privado proactivo (no cuenta como "el bot respondió al usuario")
                last_privado_enviado[uid] = datetime.now()
                await safe_send(LOGS_CHANNEL, f"✉️ Enviado MSG_PRIVADO a {mention_html}", parse_mode="HTML")
        except Exception as e:
            await safe_send(LOGS_CHANNEL, f"❌ No se pudo enviar privado a {mention_html}: {html.escape(str(e))}", parse_mode="HTML")

    except Exception:
        return

# ---------- inicio ----------
async def main():
    global ME_ID
    try:
        await client.start(phone=PHONENUMBER)
    except Exception as e:
        print("Error iniciando sesión:", e)
        return

    try:
        me = await client.get_me()
        ME_ID = getattr(me, "id", None)
    except Exception:
        ME_ID = None

    try:
        await safe_send(LOGS_CHANNEL, "<b>✅ Bot (respuestas-only) encendido — logs con username en bienvenida</b>", parse_mode="HTML")
    except Exception:
        pass

    print("Bot iniciado. Escuchando mensajes...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
