#!/usr/bin/env python3
# generar_strings_env_clean.py
# Requisitos: pip install telethon python-dotenv
"""
Genera STRING_SESSION por cada cuenta encontrada en .env y:
 - imprime en pantalla el bloque listo para copiar/pegar (KEY="VALUE")
 - limpia duplicados en .env y añade solo las nuevas definiciones
 - exporta un archivo plano: <ENV_FILE>.sessions.txt
"""

from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon import errors
from dotenv import load_dotenv
import os, re, time, shutil
from datetime import datetime
import getpass

load_dotenv()

MAX_ACCOUNTS = int(os.getenv("MAX_ACCOUNTS") or 20)
ENV_FILE = os.getenv("ENV_FILE") or ".env"
EXPORT_FILE = ENV_FILE + ".sessions.txt"

# ----------------- utilidades para .env -----------------
def read_env_text(path):
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_env_text_atomic(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

def backup_env_file(path):
    if os.path.exists(path):
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        bak = f"{path}.bak.{stamp}"
        try:
            shutil.copy2(path, bak)
            print(f"Backup creado: {bak}")
            return bak
        except Exception as e:
            print(f"ERROR backup: {e}")
    return None

def remove_keys_from_env(env_text, keys):
    """Eliminar líneas KEY=... para las claves indicadas (no borra comentarios)."""
    lines = env_text.splitlines()
    out = []
    pattern = re.compile(r'^\s*([A-Z0-9_]+)\s*=')
    for line in lines:
        m = pattern.match(line)
        if m and m.group(1) in keys:
            continue
        out.append(line)
    # devolver con salto final
    txt = "\n".join(out)
    if txt and not txt.endswith("\n"):
        txt += "\n"
    return txt

# ----------------- buscar cuentas en .env -----------------
def find_accounts(max_acc=MAX_ACCOUNTS):
    accounts = []
    for i in range(1, max_acc + 1):
        if i == 1:
            raw_id = os.getenv("API_ID"); api_hash = os.getenv("API_HASH"); phone = os.getenv("PHONENUMBER")
        else:
            raw_id = os.getenv(f"API_ID{i}"); api_hash = os.getenv(f"API_HASH{i}"); phone = os.getenv(f"PHONENUMBER{i}")
        if not (raw_id and api_hash and phone):
            continue
        try:
            api_id = int(str(raw_id).strip())
        except Exception:
            print(f"Advertencia: API_ID inválido para cuenta {i}: {raw_id}  -> se omite")
            continue
        accounts.append({"idx": i, "api_id": api_id, "api_hash": api_hash.strip(), "phone": phone.strip()})
    return accounts

# ----------------- generación interactiva -----------------
def generate_sessions(accounts, pause_between=0.8):
    generated = {}   # key -> session_str
    info_map = {}    # key -> (phone, username, user_id, len)
    for acc in accounts:
        idx = acc["idx"]
        api_id = acc["api_id"]
        api_hash = acc["api_hash"]
        phone = acc["phone"]
        key = f"STRING_SESSION{idx}"

        print(f"\n--- Cuenta {idx}: {phone} (key: {key}) ---")
        client = None
        try:
            client = TelegramClient(StringSession(), api_id, api_hash)
            client.connect()
            if not client.is_user_authorized():
                try:
                    client.send_code_request(phone)
                except Exception as e:
                    print(f"Error al solicitar código a {phone}: {e}")
                    if client:
                        try: client.disconnect()
                        except: pass
                    continue

                print("Se solicitó código. Revisa Telegram/SMS.")
                code = input("Introduce el código (ENTER para saltar): ").strip()
                if not code:
                    print("Saltando cuenta.")
                    client.disconnect()
                    continue
                try:
                    client.sign_in(phone=phone, code=code)
                except errors.SessionPasswordNeededError:
                    pwd = getpass.getpass("2FA activa. Ingresa la contraseña (oculta): ")
                    try:
                        client.sign_in(password=pwd)
                    except Exception as e:
                        print(f"Falló sign_in con 2FA: {e}")
                        client.disconnect()
                        continue
                except Exception as e:
                    print(f"sign_in falló: {e}")
                    client.disconnect()
                    continue

            # si está autorizado, guardamos la session
            s = client.session.save()
            # intentar obtener info del usuario para identificar
            try:
                me = client.get_me()
                uname = getattr(me, "username", None)
                uid = getattr(me, "id", None)
            except Exception:
                uname = None; uid = None

            generated[key] = s
            info_map[key] = (phone, uname, uid, len(s))
            print(f"{key} generado -> @{uname or 'None'} id={uid or 'None'} len={len(s)}")
            try:
                client.disconnect()
            except: pass
            time.sleep(pause_between)
        except Exception as e:
            print(f"ERROR al generar {key} para {phone}: {e}")
            try:
                if client:
                    client.disconnect()
            except:
                pass
            continue
    return generated, info_map

# ----------------- escribir resultado limpio -----------------
def write_results(generated, info_map):
    if not generated:
        print("No hay sessions para escribir.")
        return

    # Mostrar resumen y bloque para copiar
    print("\n--- RESUMEN ---")
    for k, v in info_map.items():
        phone, uname, uid, ln = v
        print(f"{k}: phone={phone} user=@{uname or 'None'} id={uid or 'None'} len={ln}")

    print("\n--- BLOQUE PARA COPIAR/PEGAR (formato .env) ---\n")
    for k, s in generated.items():
        print(f'{k}="{s}"')
    print("\n(Se exportará el mismo bloque a file:", EXPORT_FILE, ")")

    # export plano para copiar/pegar
    try:
        with open(EXPORT_FILE, "w", encoding="utf-8") as f:
            for k, s in generated.items():
                f.write(f'{k}="{s}"\n')
        print("Exportado:", EXPORT_FILE)
    except Exception as e:
        print("No se pudo exportar file:", e)

    # limpiar duplicados en .env y añadir nuevas líneas
    env_text = read_env_text(ENV_FILE)
    backup_env_file(ENV_FILE)
    keys = set(generated.keys())
    cleaned = remove_keys_from_env(env_text, keys)

    # asegurar separación visual antes de anexar
    if not cleaned.endswith("\n\n"):
        cleaned = cleaned.rstrip("\n") + "\n\n"

    parts = [cleaned]
    for k, s in generated.items():
        parts.append(f"# === STRING_SESSION generada ({k}) ===")
        parts.append(f'{k}="{s}"')
        parts.append("")

    new_env = "\n".join(parts)
    if not new_env.endswith("\n"):
        new_env += "\n"

    try:
        write_env_text_atomic(ENV_FILE, new_env)
        print(f"Se escribieron {len(generated)} keys en {ENV_FILE} (backup creado).")
    except Exception as e:
        print("ERROR al escribir .env:", e)

# ----------------- main -----------------
def main():
    accounts = find_accounts()
    if not accounts:
        print("No se encontraron cuentas válidas (verifica API_ID/API_HASH/PHONENUMBER en .env).")
        return
    print(f"Se encontraron {len(accounts)} cuentas válidas. Empezando generación...\n")
    generated, info_map = generate_sessions(accounts)
    write_results(generated, info_map)
    print("\nFIN. No compartas estas cadenas públicamente.")

if __name__ == "__main__":
    main()
