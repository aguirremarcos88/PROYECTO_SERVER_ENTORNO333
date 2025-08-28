# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import uuid
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional
import atexit

import requests
from asyncua import Client, ua
from dash import Dash, dcc, html
from dash import dash_table
from dash.dependencies import Input, Output
from flask import Response
from openpyxl import load_workbook

# ===== DB driver preferido =====
try:
    import cx_Oracle as db  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    import oracledb as db  # type: ignore

# ===== Excepción opcua segun versión =====
try:
    from asyncua.ua.uaerrors import BadNodeIdUnknown  # type: ignore
except Exception:  # pragma: no cover
    try:
        from asyncua.ua.uaerrors._auto import BadNodeIdUnknown  # type: ignore
    except Exception:  # pragma: no cover
        class BadNodeIdUnknown(Exception):
            ...

# ===== (Opcional) .env =====
try:  # pragma: no cover
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

BASE_DIR = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
if load_dotenv:
    load_dotenv(BASE_DIR / ".env")

# --------------------------------------------------------------------------------------
# Config util
# --------------------------------------------------------------------------------------

def env_str(name: str, default: str) -> str:
    return os.getenv(name, default)

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "t", "yes", "y"}

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    url: str = env_str("OPCUA_URL", "opc.tcp://localhost:52250/freeopcua/server/")
    node_groups: list[str] = field(
        default_factory=lambda: [g.strip() for g in env_str("NODE_GROUPS", "BOMBAS_SH,CONVEYOR_SH").split(",") if g.strip()]
    )
    ns: int = env_int("OPCUA_NS", 2)
    start_hex: int = int(env_str("START_HEX", "0x80"), 16)
    end_hex: int = int(env_str("END_HEX", "0xFF"), 16)

    sampling_ms: int = env_int("SAMPLING_MS", 100)
    sub_queue_size: int = env_int("SUB_QUEUE_SIZE", 1000)
    sub_keepalive_count: int = env_int("SUB_KEEPALIVE_COUNT", 100)
    sub_lifetime_count: int = env_int("SUB_LIFETIME_COUNT", sub_keepalive_count * 10)

    ui_interval_ms: int = int(float(env_str("INTERVALO_SEGUNDOS", "0.5")) * 1000)

    log_level: str = env_str("LOG_LEVEL", "INFO").upper()
    log_file: str = str((BASE_DIR / "monitor.log").resolve())

    telegram_enabled: bool = env_bool("TELEGRAM_ENABLED", True)
    telegram_token: Optional[str] = os.getenv("TELEGRAM_TOKEN")
    telegram_chat_ids: list[str] = field(
        default_factory=lambda: [x.strip() for x in os.getenv("TELEGRAM_CHAT_IDS", "1600754452,5015132163").split(",") if x.strip()]
    )
    telegram_threshold_seconds: int = env_int("TELEGRAM_THRESHOLD_SECONDS", 60)
    telegram_restablecido_unico: bool = env_bool("TELEGRAM_RESTABLECIDO_UNICO", True)

    host: str = env_str("HOST", "0.0.0.0")
    port: int = env_int("PORT", 8050)

    # === GATE TOTAL_START ===
    gate_group: str = env_str("GATE_GROUP", "CONVEYOR_SH")
    gate_node: str = env_str("GATE_NODE", "TOTAL_START")

CFG = Config()

# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------

logger = logging.getLogger("opcua-monitor")
logger.setLevel(CFG.log_level)
logger.propagate = False
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
logger.handlers.clear()
fh = RotatingFileHandler(CFG.log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
fh.setFormatter(fmt)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
logger.addHandler(fh)
logger.addHandler(sh)

# --------------------------------------------------------------------------------------
# Oracle client
# --------------------------------------------------------------------------------------

def _oracle_conn():
    user = os.getenv("ORACLE_USER")
    pwd = os.getenv("ORACLE_PWD")
    dsn = os.getenv("ORACLE_DSN")
    if not all([user, pwd, dsn]):
        raise RuntimeError("Faltan ORACLE_USER/ORACLE_PWD/ORACLE_DSN en .env")
    return db.connect(user=user, password=pwd, dsn=dsn)

@contextmanager
def oracle_cursor():
    conn = _oracle_conn()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    finally:
        with suppress(Exception):
            cur.close()
            conn.close()

class OracleRepo:
    def ensure_schema(self):
        try:
            with oracle_cursor() as cur:
                cur.execute(
                    """
                    BEGIN
                        EXECUTE IMMEDIATE '
                            CREATE TABLE MONITOREO_OPCUA (
                                SESSION_ID    VARCHAR2(64),
                                TIPO          VARCHAR2(30),
                                GRUPO         VARCHAR2(50),
                                INICIO        TIMESTAMP,
                                FIN           TIMESTAMP,
                                MENSAJE       VARCHAR2(4000)
                            )';
                    EXCEPTION WHEN OTHERS THEN
                        IF SQLCODE != -955 THEN RAISE; END IF;
                    END;"""
                )
                with suppress(Exception):
                    cur.execute(
                        """
                        BEGIN
                            EXECUTE IMMEDIATE 'ALTER TABLE MONITOREO_OPCUA ADD (EVENT_ID VARCHAR2(64))';
                        EXCEPTION WHEN OTHERS THEN
                            IF SQLCODE != -1430 THEN RAISE; END IF;
                        END;"""
                    )
        except Exception as e:  # pragma: no cover
            logger.error(f"Error asegurando tablas Oracle: {e}")

    def start_monitoring(self, session_id: str, inicio: datetime):
        with suppress(Exception):
            with oracle_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO MONITOREO_OPCUA (SESSION_ID, TIPO, GRUPO, INICIO, FIN, MENSAJE)
                    VALUES (:sid, 'MONITOREO', NULL, :ini, :fin, :msg)
                    """,
                    dict(sid=session_id, ini=inicio, fin=inicio, msg=None),
                )

    def tick_monitoring(self, session_id: str, fin: datetime):
        with suppress(Exception):
            with oracle_cursor() as cur:
                cur.execute(
                    """UPDATE MONITOREO_OPCUA SET FIN = :fin WHERE SESSION_ID = :sid AND TIPO = 'MONITOREO'""",
                    dict(fin=fin, sid=session_id),
                )

    def insert_error_conn(self, session_id: str, inicio: datetime, fin: datetime, msg: str):
        with suppress(Exception):
            with oracle_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO MONITOREO_OPCUA (SESSION_ID, TIPO, GRUPO, INICIO, FIN, MENSAJE)
                    VALUES (:sid, 'ERROR_CONEXION', NULL, :ini, :fin, :msg)
                    """,
                    dict(sid=session_id, ini=inicio, fin=fin, msg=(msg or "")[:3999]),
                )

    def open_group_error(self, session_id: str, grupo: str, inicio: datetime, msg: str, eid: Optional[str]):
        with suppress(Exception):
            with oracle_cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO MONITOREO_OPCUA (SESSION_ID, TIPO, GRUPO, INICIO, FIN, MENSAJE, EVENT_ID)
                        VALUES (:sid, 'ERROR_GRUPO', :grp, :ini, NULL, :msg, :eid)
                        """,
                        dict(sid=session_id, grp=grupo, ini=inicio, msg=(msg or "")[:3999], eid=eid),
                    )
                except Exception:
                    cur.execute(
                        """
                        INSERT INTO MONITOREO_OPCUA (SESSION_ID, TIPO, GRUPO, INICIO, FIN, MENSAJE)
                        VALUES (:sid, 'ERROR_GRUPO', :grp, :ini, NULL, :msg)
                        """,
                        dict(sid=session_id, grp=grupo, ini=inicio, msg=(msg or "")[:3999]),
                    )

    def close_group_error(self, session_id: str, grupo: str, fin: datetime, eid: Optional[str]):
        with suppress(Exception):
            with oracle_cursor() as cur:
                if eid:
                    cur.execute(
                        """
                        UPDATE MONITOREO_OPCUA SET FIN = :fin
                         WHERE SESSION_ID = :sid AND TIPO = 'ERROR_GRUPO' AND GRUPO = :grp AND EVENT_ID = :eid AND FIN IS NULL
                        """,
                        dict(fin=fin, sid=session_id, grp=grupo, eid=eid),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE MONITOREO_OPCUA SET FIN = :fin
                         WHERE SESSION_ID = :sid AND TIPO = 'ERROR_GRUPO' AND GRUPO = :grp AND FIN IS NULL
                        """,
                        dict(fin=fin, sid=session_id, grp=grupo),
                    )

ORACLE = OracleRepo()
ORACLE.ensure_schema()

# --------------------------------------------------------------------------------------
# Telegram client
# --------------------------------------------------------------------------------------

class TelegramClient:
    def __init__(self, enabled: bool, token: Optional[str], chat_ids: list[str]) -> None:
        self.enabled = enabled and bool(token) and any(cid.strip().lstrip("-").isdigit() for cid in chat_ids)
        self.token = token
        self.chat_ids = [c.strip() for c in chat_ids if c.strip()]
        self.session = requests.Session()

    def send(self, text: str):
        if not self.enabled or not self.token:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        for cid in self.chat_ids:
            payload = dict(chat_id=cid, text=text, parse_mode="HTML", disable_web_page_preview=True)
            try:
                r = self.session.post(url, json=payload, timeout=(3, 8))
                if r.status_code != 200:
                    logger.error(f"Telegram error chat_id={cid}: {r.status_code} {r.text}")
            except Exception as e:  # pragma: no cover
                logger.error(f"Telegram excepción chat_id={cid}: {e}")

    def close(self):
        with suppress(Exception):
            self.session.close()

TELEGRAM = TelegramClient(CFG.telegram_enabled, CFG.telegram_token, CFG.telegram_chat_ids)
atexit.register(TELEGRAM.close)

# --------------------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------------------

@dataclass
class State:
    node_groups: list[str]
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    lock: threading.Lock = field(default_factory=threading.Lock)

    # Timers y datos GMF
    tiempos_gmf: dict[str, datetime] = field(default_factory=dict)
    tiempos_congelados: dict[str, int] = field(default_factory=dict)

    # Acumuladores OFF (Dash vs Telegram)
    acumulado_continuous: dict[str, timedelta] = field(default_factory=dict)           # visible en UI (se congela con gate OFF)
    ultima_marca_continuous: dict[str, datetime] = field(default_factory=dict)
    acumulado_continuous_tele: dict[str, timedelta] = field(default_factory=dict)      # interno (sigue contando con gate OFF)
    ultima_marca_continuous_tele: dict[str, datetime] = field(default_factory=dict)

    descripciones_gmf: dict[str, dict[str, str]] = field(default_factory=dict)

    tg_enviado: dict[str, bool] = field(init=False)
    tg_restaurado_enviado: dict[str, bool] = field(init=False)

    last_mon_update: datetime = field(default_factory=lambda: datetime.min)

    error_activo: bool = False
    error_inicio: Optional[datetime] = None
    ultimo_error_msg: str = ""
    error_grupo_activo: dict[str, bool] = field(init=False)
    error_grupo_inicio: dict[str, Optional[datetime]] = field(init=False)
    error_grupo_msg: dict[str, str] = field(init=False)
    error_grupo_event_id: dict[str, Optional[str]] = field(init=False)

    current_continuous: dict[str, bool] = field(init=False)
    state_cont_known: dict[str, bool] = field(init=False)
    last_word_values: dict[tuple[str, str], int] = field(default_factory=dict)

    # === GATE TOTAL_START ===
    gate_open: bool = True
    gate_known: bool = False
    gate_last_change: Optional[datetime] = None

    # === TOTAL_FAULT por equipo ===
    current_total_fault: dict[str, bool] = field(init=False)
    state_tf_known: dict[str, bool] = field(init=False)

    def __post_init__(self):
        self.tg_enviado = {g: False for g in self.node_groups}
        self.tg_restaurado_enviado = {g: False for g in self.node_groups}
        self.error_grupo_activo = {g: False for g in self.node_groups}
        self.error_grupo_inicio = {g: None for g in self.node_groups}
        self.error_grupo_msg = {g: "" for g in self.node_groups}
        self.error_grupo_event_id = {g: None for g in self.node_groups}
        self.current_continuous = {g: True for g in self.node_groups}
        self.state_cont_known = {g: False for g in self.node_groups}
        self.current_total_fault = {g: False for g in self.node_groups}
        self.state_tf_known = {g: False for g in self.node_groups}

    # ---- helpers ----
    def marcar_error_grupo(self, grupo: str, mensaje: str, inicio: Optional[datetime] = None):
        ahora = inicio or datetime.now()
        with self.lock:
            ya_activo = self.error_grupo_activo.get(grupo, False)
            self.error_grupo_activo[grupo] = True
            if not ya_activo:
                self.error_grupo_inicio[grupo] = ahora
                eid = uuid.uuid4().hex
                self.error_grupo_event_id[grupo] = eid
            else:
                eid = self.error_grupo_event_id.get(grupo)
            self.error_grupo_msg[grupo] = mensaje
        if not ya_activo:
            ORACLE.open_group_error(self.session_id, grupo, ahora, mensaje, eid)
            logger.warning(f"ERROR_GRUPO abierto: {grupo} @ {ahora} | {mensaje} | event_id={eid}")

    def limpiar_error_grupo(self, grupo: str, fin: Optional[datetime] = None):
        ahora = fin or datetime.now()
        with self.lock:
            estaba = self.error_grupo_activo.get(grupo, False)
            eid = self.error_grupo_event_id.get(grupo)
            self.error_grupo_activo[grupo] = False
            self.error_grupo_event_id[grupo] = None
        if estaba:
            ORACLE.close_group_error(self.session_id, grupo, ahora, eid)
            logger.info(f"ERROR_GRUPO cerrado: {grupo} @ {ahora} | event_id={eid}")

STATE = State(CFG.node_groups)

# --------------------------------------------------------------------------------------
# Utilidades
# --------------------------------------------------------------------------------------

def nid(s: str) -> str:
    return f"ns={CFG.ns};s={s}"

def determinar_turno(fecha: datetime) -> str:
    hora = fecha.time()
    if time(6, 5) <= hora <= time(14, 30):
        return "MAÑANA"
    if time(14, 39) <= hora <= time(22, 54):
        return "TARDE"
    if time(23, 13) <= hora <= time(23, 59) or time(0, 0) <= hora <= time(5, 55):
        return "NOCHE"
    return "FUERA DE TURNO"

def _fmt_mmss(segundos: int) -> str:
    m, s = divmod(max(0, int(segundos)), 60)
    return f"{m:02d}:{s:02d}"

def _normalize_code(codigo) -> str:
    if codigo is None:
        return ""
    if isinstance(codigo, (int, float)):
        s = f"{codigo}"
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s.strip().upper()
    return str(codigo).strip().upper()

def _desc_gmf(grupo: str, gmf: str) -> str:
    return STATE.descripciones_gmf.get(grupo, {}).get(gmf.strip().upper(), "Sin descripción")

def _msg_telegram(grupo: str, segundos: int, activos: list[str]) -> str:
    import html as html_escape
    if not activos:
        return f"⚠️ {html_escape.escape(grupo)} en falla, hace {segundos} s."
    if len(activos) == 1:
        g = activos[0]
        d = _desc_gmf(grupo, g)
        return f"⚠️ {html_escape.escape(grupo)} con falla {html_escape.escape(g)}: {html_escape.escape(d)} , hace {segundos} s."
    partes = [f"{html_escape.escape(g)}: {html_escape.escape(_desc_gmf(grupo, g))}" for g in activos]
    listado = " // ".join(partes[:10])
    extra = "" if len(partes) <= 10 else f" (+{len(partes)-10} más)"
    return f"⚠️ {html_escape.escape(grupo)} con fallas <b>{listado}{extra}</b> , hace {segundos} s."

# === helpers GMF bits ===
def _split_gmf_code(gmf: str) -> tuple[str, int]:
    base = gmf[3:].upper()
    return base[:-1], int(base[-1], 16)

def _is_gmf_active_now(grupo: str, gmf: str) -> bool:
    word_suffix, bit = _split_gmf_code(gmf)
    val = STATE.last_word_values.get((grupo, word_suffix), 0)
    return bool((val >> bit) & 1)

# --------------------------------------------------------------------------------------
# Carga de descripciones (Excel)
# --------------------------------------------------------------------------------------

ARCHIVOS_DESCRIPCIONES = {
    "BOMBAS_SH": str((BASE_DIR / "GMFs_de_Equipos" / "GMF_BOMBAS_SH.xlsx").resolve()),
    "CONVEYOR_SH": str((BASE_DIR / "GMFs_de_Equipos" / "GMF_CONVEYOR_SH.xlsx").resolve()),
}

def cargar_descripciones():
    for grupo, ruta in ARCHIVOS_DESCRIPCIONES.items():
        STATE.descripciones_gmf[grupo] = {}
        try:
            wb = load_workbook(ruta, data_only=True)
            hoja = wb.active
            for fila in hoja.iter_rows(min_row=2, values_only=True):
                codigo_raw, descripcion = fila[:2]
                if codigo_raw and descripcion:
                    codigo = _normalize_code(codigo_raw)
                    if not codigo.startswith("GMF"):
                        codigo = "GMF" + codigo
                    STATE.descripciones_gmf[grupo][codigo] = str(descripcion).strip()
            logger.info(f"[Desc] {grupo}: {len(STATE.descripciones_gmf[grupo])} códigos desde {ruta}")
        except Exception as e:  # pragma: no cover
            logger.error(f"Error cargando descripciones {grupo}: {e}")

# --------------------------------------------------------------------------------------
# Exportar a ORACLE (historial de fallas)
# --------------------------------------------------------------------------------------

def exportar_a_oracle(grupo: str, gmf: str, descripcion: str, inicio: datetime, fin: datetime, acumulado: int):
    try:
        tabla = f"HISTORIAL_FALLAS_{grupo.upper()}"
        with oracle_cursor() as cur:
            cur.execute(
                f"""
                BEGIN
                    EXECUTE IMMEDIATE '
                        CREATE TABLE {tabla} (
                            EQUIPO           VARCHAR2(50),
                            FALLA            VARCHAR2(50),
                            DESCRIPCION      VARCHAR2(255),
                            INICIO_FALLA     TIMESTAMP,
                            FIN_FALLA        TIMESTAMP,
                            TIEMPO_OFF_SEG   NUMBER,
                            TURNO            VARCHAR2(20)
                        )';
                EXCEPTION WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF;
                END;"""
            )
            cur.execute(
                f"""
                INSERT INTO {tabla} (EQUIPO, FALLA, DESCRIPCION, INICIO_FALLA, FIN_FALLA, TIEMPO_OFF_SEG, TURNO)
                VALUES (:1, :2, :3, :4, :5, :6, :7)
                """,
                (grupo, gmf, descripcion, inicio, fin, acumulado, determinar_turno(inicio)),
            )
        logger.info(f"Exportado a Oracle: {grupo} | {gmf} | {acumulado} seg")
    except Exception as e:  # pragma: no cover
        logger.error(f"Error exportando a Oracle: {e}")

# --------------------------------------------------------------------------------------
# Exportar congeladas al volver CONTINUOUS=1 (con reset)
# --------------------------------------------------------------------------------------

def exportar_congeladas_de_grupo(grupo: str, ahora: datetime):
    """Exporta a Oracle las fallas 'congeladas' de un grupo (cuando CONTINUOUS estuvo en 0)
    y resetea el acumulado OFF del grupo (Dash).
    """
    with STATE.lock:
        pendientes = [
            (k.split(".")[1], seg)
            for k, seg in STATE.tiempos_congelados.items()
            if k.startswith(grupo + ".")
        ]
        segundos_off = int(STATE.acumulado_continuous.get(grupo, timedelta()).total_seconds())

    for gmf, seg in pendientes:
        try:
            inicio = ahora - timedelta(seconds=int(seg))
            descripcion = _desc_gmf(grupo, gmf)
            exportar_a_oracle(grupo, gmf, descripcion, inicio, ahora, segundos_off)
            with STATE.lock:
                STATE.tiempos_congelados.pop(f"{grupo}.{gmf}", None)
        except Exception as e:  # pragma: no cover
            logger.error(f"Error exportando congelada {grupo}.{gmf}: {e}")

    with STATE.lock:
        STATE.acumulado_continuous[grupo] = timedelta(0)

# --------------------------------------------------------------------------------------
# Node cache (con manejo dinámico de GMF por TOTAL_FAULT)
# --------------------------------------------------------------------------------------

class NodeCache:
    def __init__(self, client: Client):
        self.client = client
        self.cache: dict[str, dict] = {}

    def build_for_groups(self, grupos: list[str]):
        for grupo in grupos:
            total_fault = self.client.get_node(nid(f"{grupo}.TOTAL_FAULT"))
            continuous = self.client.get_node(nid(f"{grupo}.CONTINUOUS"))
            total_start = self.client.get_node(nid(f"{grupo}.TOTAL_START"))
            hex_suffixes = [f"{i:X}" for i in range(CFG.start_hex, CFG.end_hex + 1)]
            node_ids = [ua.NodeId.from_string(nid(f"{grupo}.GMF{suf}")) for suf in hex_suffixes]
            gmf_nodes = [self.client.get_node(n) for n in node_ids]
            self.cache[grupo] = {
                "total_fault": total_fault,
                "continuous": continuous,
                "total_start": total_start,
                "gmf_nodes": gmf_nodes,
                "gmf_suffixes": hex_suffixes,
                "gmf_validated": False,
                # suscripción y handle GMF dinámicos
                "subscription": None,
                "gmf_handle": None,
            }
        return self

    def for_group(self, grupo: str) -> dict:
        return self.cache[grupo]

    # Alta/baja dinámica de GMF por grupo
    async def enable_gmf_for_group(self, grupo: str, queuesize: int):
        c = self.cache.get(grupo)
        if not c or not c.get("subscription"):
            return
        if c.get("gmf_handle") is not None:
            return
        if not c.get("gmf_nodes"):
            return
        try:
            h = await c["subscription"].subscribe_data_change(c["gmf_nodes"], queuesize=queuesize)
            c["gmf_handle"] = h
            logger.info(f"[{grupo}] GMF suscritas ({len(c['gmf_nodes'])} ítems)")
        except Exception as e:
            logger.warning(f"[{grupo}] No se pudo suscribir GMF dinámicamente: {e}")

    async def disable_gmf_for_group(self, grupo: str):
        c = self.cache.get(grupo)
        if not c or not c.get("subscription"):
            return
        h = c.get("gmf_handle")
        if h is None:
            return
        try:
            await c["subscription"].unsubscribe(h)
            c["gmf_handle"] = None
            logger.info(f"[{grupo}] GMF desuscritas")
        except Exception as e:
            logger.warning(f"[{grupo}] No se pudo desuscribir GMF: {e}")

# --------------------------------------------------------------------------------------
# Subscription handler
# --------------------------------------------------------------------------------------

class SubHandler:
    def __init__(self, client: Client, ncache: NodeCache):
        self.client = client
        self.ncache = ncache

    async def _cerrar_falla(self, grupo: str, gmf_codigo: str, ahora: datetime):
        try:
            descripcion = _desc_gmf(grupo, gmf_codigo)
            with STATE.lock:
                inicio_falla = STATE.tiempos_gmf.get(f"{grupo}.{gmf_codigo}")
                acumulado = int(STATE.acumulado_continuous.get(grupo, timedelta()).total_seconds())
                cont_ok = bool(STATE.current_continuous.get(grupo, True)) and STATE.state_cont_known.get(grupo, False)

            if cont_ok:
                exportar_a_oracle(grupo, gmf_codigo, descripcion, inicio_falla, ahora, acumulado)
                with STATE.lock:
                    STATE.acumulado_continuous[grupo] = timedelta(0)
                if (acumulado > CFG.telegram_threshold_seconds) and (not STATE.tg_restaurado_enviado.get(grupo, False)):
                    TELEGRAM.send(f'✅ Equipo "{grupo}" restablecido. Tiempo acumulado {int(max(0, acumulado))} s')
                    if CFG.telegram_restablecido_unico:
                        STATE.tg_restaurado_enviado[grupo] = True
                with STATE.lock:
                    STATE.tiempos_gmf.pop(f"{grupo}.{gmf_codigo}", None)
                    STATE.tiempos_congelados.pop(f"{grupo}.{gmf_codigo}", None)
            else:
                with STATE.lock:
                    clave = f"{grupo}.{gmf_codigo}"
                    if clave in STATE.tiempos_gmf:
                        segs = int((ahora - STATE.tiempos_gmf[clave]).total_seconds())
                        STATE.tiempos_congelados[clave] = segs
                        STATE.tiempos_gmf.pop(clave, None)
        except Exception as e:  # pragma: no cover
            logger.error(f"Error cerrando falla {grupo}.{gmf_codigo}: {e}")

    # --- Gate helpers ---
    def _freeze_all(self, ahora: datetime):
        with STATE.lock:
            activos = list(STATE.tiempos_gmf.items())
        for gmf_id, inicio in activos:
            try:
                segs = max(0, int((ahora - inicio).total_seconds()))
                with STATE.lock:
                    STATE.tiempos_congelados[gmf_id] = max(STATE.tiempos_congelados.get(gmf_id, 0), segs)
                    STATE.tiempos_gmf.pop(gmf_id, None)
            except Exception:
                pass
        logger.info(f"TOTAL_START=0 → congeladas {len(activos)} fallas y pausado Tiempo OFF (Dash).")

    def _unfreeze_all(self, ahora: datetime):
        with STATE.lock:
            congeladas = list(STATE.tiempos_congelados.items())
        cerradas = 0
        reactivadas = 0
        for gmf_id, segs in congeladas:
            equipo, gmf = gmf_id.split(".")
            if _is_gmf_active_now(equipo, gmf):
                with STATE.lock:
                    STATE.tiempos_gmf[gmf_id] = ahora - timedelta(seconds=int(segs))
                    STATE.tiempos_congelados.pop(gmf_id, None)
                reactivadas += 1
            else:
                with STATE.lock:
                    cont_ok = bool(STATE.current_continuous.get(equipo, True)) and STATE.state_cont_known.get(equipo, False)
                    segundos_off = int(STATE.acumulado_continuous.get(equipo, timedelta()).total_seconds())
                if cont_ok:
                    try:
                        exportar_a_oracle(equipo, gmf, _desc_gmf(equipo, gmf), ahora - timedelta(seconds=int(segs)), ahora, segundos_off)
                        with STATE.lock:
                            STATE.tiempos_congelados.pop(gmf_id, None)
                            STATE.acumulado_continuous[equipo] = timedelta(0)
                        cerradas += 1
                    except Exception as e:
                        logger.error(f"Error exportando (unfreeze) {gmf_id}: {e}")
        logger.info(f"TOTAL_START=1 → reactivadas {reactivadas}, cerradas {cerradas}.")

    def _flush_or_freeze_group(self, grupo: str, ahora: datetime):
        """Si CONT=1 exporta y limpia; si CONT=0 congela lo activo del grupo."""
        with STATE.lock:
            cont_ok = bool(STATE.current_continuous.get(grupo, True)) and STATE.state_cont_known.get(grupo, False)
            activos = [(k, v) for k, v in STATE.tiempos_gmf.items() if k.startswith(grupo + ".")]
            segundos_off = int(STATE.acumulado_continuous.get(grupo, timedelta()).total_seconds())

        cerradas = 0
        congeladas = 0
        for gmf_id, inicio in activos:
            equipo, gmf = gmf_id.split(".")
            if cont_ok:
                try:
                    exportar_a_oracle(equipo, gmf, _desc_gmf(equipo, gmf), inicio, ahora, segundos_off)
                except Exception as e:
                    logger.error(f"Error exportando por TF=0 {gmf_id}: {e}")
                with STATE.lock:
                    STATE.tiempos_gmf.pop(gmf_id, None)
                cerradas += 1
            else:
                segs = max(0, int((ahora - inicio).total_seconds()))
                with STATE.lock:
                    STATE.tiempos_congelados[gmf_id] = max(STATE.tiempos_congelados.get(gmf_id, 0), segs)
                    STATE.tiempos_gmf.pop(gmf_id, None)
                congeladas += 1

        if cont_ok and (cerradas > 0):
            with STATE.lock:
                STATE.acumulado_continuous[grupo] = timedelta(0)

        logger.info(f"[{grupo}] TOTAL_FAULT=0 → cerradas {cerradas}, congeladas {congeladas}.")

    # === Gate TOTAL_START
    def _on_gate_total_start(self, val_bool: bool, ahora: datetime):
        prev = STATE.gate_open
        STATE.gate_open = bool(val_bool)
        STATE.gate_known = True
        STATE.gate_last_change = ahora

        # Rebase SOLO del acumulador Dash (no tocamos TELE)
        with STATE.lock:
            for g in CFG.node_groups:
                STATE.ultima_marca_continuous[g] = ahora

        if prev and not STATE.gate_open:
            # ❗ No desuscribimos GMF: necesitamos escuchar caídas para exportar en caliente
            self._freeze_all(ahora)
        elif (not prev) and STATE.gate_open:
            self._unfreeze_all(ahora)
            # Nada que habilitar/deshabilitar aquí; TF maneja GMF por grupo

    # === TOTAL_FAULT por equipo
    def _on_total_fault(self, grupo: str, val_bool: bool, ahora: datetime):
        prev = STATE.current_total_fault.get(grupo, False)
        STATE.current_total_fault[grupo] = bool(val_bool)
        STATE.state_tf_known[grupo] = True

        if STATE.gate_known and (not STATE.gate_open):
            # Gate cerrado: mantenemos GMF suscritas.
            # Si TF=0 y CONT=1, exportamos congeladas del grupo ahora mismo.
            if not val_bool:
                cont_ok = STATE.state_cont_known.get(grupo, False) and bool(STATE.current_continuous.get(grupo, True))
                if cont_ok:
                    exportar_congeladas_de_grupo(grupo, ahora)  # también resetea acumulado Dash
            return

        # Gate abierto: dinámica normal por TF
        if val_bool:
            asyncio.create_task(self.ncache.enable_gmf_for_group(grupo, CFG.sub_queue_size))
        else:
            asyncio.create_task(self.ncache.disable_gmf_for_group(grupo))
            self._flush_or_freeze_group(grupo, ahora)

    # === CONTINUOUS
    def _on_continuous(self, grupo: str, val_bool: bool, ahora: datetime):
        prev = STATE.current_continuous.get(grupo)
        STATE.current_continuous[grupo] = val_bool
        STATE.state_cont_known[grupo] = True

        # Mantener marcas (Dash y Tele)
        with STATE.lock:
            STATE.acumulado_continuous.setdefault(grupo, timedelta(0))
            STATE.acumulado_continuous_tele.setdefault(grupo, timedelta(0))
            segs_off_tele_antes = int(STATE.acumulado_continuous_tele[grupo].total_seconds())
            STATE.ultima_marca_continuous[grupo] = ahora
            STATE.ultima_marca_continuous_tele[grupo] = ahora

        # 1 -> 0: permitir nuevo "restablecido"
        if prev is True and val_bool is False:
            STATE.tg_restaurado_enviado[grupo] = False

        # 0 -> 1: enviar "restablecido" si superó umbral (con TELE) sin importar gate
        if prev is False and val_bool is True:
            if (segs_off_tele_antes > CFG.telegram_threshold_seconds) and (not STATE.tg_restaurado_enviado.get(grupo, False)):
                TELEGRAM.send(f'✅ Equipo "{grupo}" restablecido. Tiempo acumulado {segs_off_tele_antes} s')
                if CFG.telegram_restablecido_unico:
                    STATE.tg_restaurado_enviado[grupo] = True
            # reset TELE al finalizar OFF
            with STATE.lock:
                STATE.acumulado_continuous_tele[grupo] = timedelta(0)

        # Gate cerrado: exporta congeladas si CONT=1, y RESETEA acumulador visible
        if not STATE.gate_open:
            if val_bool:
                with STATE.lock:
                    congeladas = [(k, v) for k, v in STATE.tiempos_congelados.items() if k.startswith(grupo + ".")]
                    segundos_off_dash = int(STATE.acumulado_continuous.setdefault(grupo, timedelta(0)).total_seconds())
                cerradas = 0
                for gmf_id, segs in congeladas:
                    equipo, gmf = gmf_id.split(".")
                    if not _is_gmf_active_now(equipo, gmf):
                        try:
                            inicio = ahora - timedelta(seconds=int(segs))
                            exportar_a_oracle(equipo, gmf, _desc_gmf(equipo, gmf), inicio, ahora, segundos_off_dash)
                            with STATE.lock:
                                STATE.tiempos_congelados.pop(gmf_id, None)
                            cerradas += 1
                        except Exception as e:
                            logger.error(f"Error exportando (CONT=1 con gate cerrado) {gmf_id}: {e}")

                # 🔧 Punto clave: si exportamos algo, reiniciar el OFF visible del grupo
                if cerradas > 0:
                    with STATE.lock:
                        STATE.acumulado_continuous[grupo] = timedelta(0)
                        # Si ya no quedan fallas (ni activas ni congeladas), liberamos bandera de spam
                        quedan_activas = any(k.startswith(grupo + ".") for k in STATE.tiempos_gmf)
                        quedan_congeladas = any(k.startswith(grupo + ".") for k in STATE.tiempos_congelados)
                        if not quedan_activas and not quedan_congeladas:
                            STATE.tg_enviado[grupo] = False
            return

        # Gate abierto: comportamiento normal
        if prev is False and val_bool is True:
            exportar_congeladas_de_grupo(grupo, ahora)
            with STATE.lock:
                quedan_activas = any(k.startswith(grupo + ".") for k in STATE.tiempos_gmf)
                quedan_congeladas = any(k.startswith(grupo + ".") for k in STATE.tiempos_congelados)
                if not quedan_activas and not quedan_congeladas:
                    STATE.tg_enviado[grupo] = False

    def _on_gmf_word(self, grupo: str, suffix_hex: str, value: int, ahora: datetime):
        key = (grupo, suffix_hex)
        prev_value = STATE.last_word_values.get(key, 0)
        STATE.last_word_values[key] = value

        prev_bits = {i for i in range(16) if (prev_value >> i) & 1}
        curr_bits = {i for i in range(16) if (value >> i) & 1}

        # Gate cerrado o TF=0 → ignoramos arranques; pero si caen bits y CONT=1, exportamos congeladas
        if (STATE.gate_known and (not STATE.gate_open)) or (STATE.state_tf_known.get(grupo, False) and (not STATE.current_total_fault.get(grupo, False))):
            falls = prev_bits - curr_bits  # 1->0
            for bit_pos in falls:
                gmf_base = f"{suffix_hex}{bit_pos:X}".upper()
                gmf = f"GMF{gmf_base}"
                gmf_id = f"{grupo}.{gmf}"

                with STATE.lock:
                    segs = STATE.tiempos_congelados.get(gmf_id)  # debe existir si se congeló al cerrar el gate
                    cont_ok = STATE.state_cont_known.get(grupo, False) and bool(STATE.current_continuous.get(grupo, True))
                    segundos_off = int(STATE.acumulado_continuous.get(grupo, timedelta()).total_seconds())

                if segs is not None and cont_ok:
                    try:
                        inicio = ahora - timedelta(seconds=int(segs))
                        exportar_a_oracle(grupo, gmf, _desc_gmf(grupo, gmf), inicio, ahora, segundos_off)
                        with STATE.lock:
                            STATE.tiempos_congelados.pop(gmf_id, None)
                            # Reset del acumulador OFF visible (Dash) del grupo tras exportar
                            STATE.acumulado_continuous[grupo] = timedelta(0)
                    except Exception as e:
                        logger.error(f"Error exportando (gate/TF cerrado) {gmf_id}: {e}")
            return

        # Gate abierto y TF=1 → normal
        for bit_pos in (curr_bits - prev_bits):
            gmf_base = f"{suffix_hex}{bit_pos:X}".upper()
            gmf_id = f"{grupo}.GMF{gmf_base}"
            with STATE.lock:
                if gmf_id not in STATE.tiempos_gmf and gmf_id not in STATE.tiempos_congelados:
                    STATE.tiempos_gmf[gmf_id] = ahora

        for bit_pos in (prev_bits - curr_bits):
            gmf_base = f"{suffix_hex}{bit_pos:X}".upper()
            asyncio.create_task(self._cerrar_falla(grupo, f"GMF{gmf_base}", ahora))

    def datachange_notification(self, node, val, data):
        try:
            ahora = datetime.now()
            nid_str = node.nodeid.to_string()
            if not isinstance(nid_str, str) or ";s=" not in nid_str:
                return
            s = nid_str.split(";s=", 1)[1]
            if "." not in s:
                return
            grupo, campo = s.split(".", 1)
            campo = campo.upper()

            # Gate TOTAL_START (general)
            if (grupo == CFG.gate_group) and (campo == CFG.gate_node.upper()):
                self._on_gate_total_start(bool(val), ahora)
                return

            if campo == "CONTINUOUS":
                self._on_continuous(grupo, bool(val), ahora)
                return

            if campo == "TOTAL_FAULT":
                self._on_total_fault(grupo, bool(val), ahora)
                return

            if campo.startswith("GMF") and isinstance(val, int):
                self._on_gmf_word(grupo, campo[3:], val, ahora)
        except Exception as e:  # pragma: no cover
            logger.error(f"Handler datachange error: {e}")

# --------------------------------------------------------------------------------------
# Suscripción por grupo
# --------------------------------------------------------------------------------------

async def _validate_gmfs(client: Client, grupo: str, ncache: dict):
    valid_nodes, valid_suffixes = [], []
    for node, suf in zip(ncache["gmf_nodes"], ncache["gmf_suffixes"]):
        try:
            await node.read_value()
            valid_nodes.append(node)
            valid_suffixes.append(suf)
        except BadNodeIdUnknown:
            logger.debug(f"{grupo}: GMF{suf} no existe, se omite.")
        except Exception:
            logger.debug(f"{grupo}: GMF{suf} no legible por ahora, se omite.")
    ncache["gmf_nodes"], ncache["gmf_suffixes"], ncache["gmf_validated"] = valid_nodes, valid_suffixes, True

async def suscribir_grupos(client: Client, ncache: NodeCache) -> list:
    subs = []
    for grupo in CFG.node_groups:
        try:
            cache_g = ncache.for_group(grupo)
            if not cache_g.get("gmf_validated", False):
                await _validate_gmfs(client, grupo, cache_g)

            # CONTINUOUS inicial
            try:
                init_val = await cache_g["continuous"].read_value()
                with STATE.lock:
                    STATE.current_continuous[grupo] = bool(init_val)
                    STATE.state_cont_known[grupo] = True
                    # Dash
                    STATE.acumulado_continuous.setdefault(grupo, timedelta(0))
                    STATE.ultima_marca_continuous[grupo] = datetime.now()
                    # Tele
                    STATE.acumulado_continuous_tele.setdefault(grupo, timedelta(0))
                    STATE.ultima_marca_continuous_tele[grupo] = datetime.now()
                logger.info(f"Init CONTINUOUS {grupo} = {bool(init_val)}")
            except Exception as e:  # pragma: no cover
                logger.warning(f"No se pudo leer CONTINUOUS inicial de {grupo}: {e}")

            # TOTAL_FAULT inicial
            try:
                init_tf = await cache_g["total_fault"].read_value()
                with STATE.lock:
                    STATE.current_total_fault[grupo] = bool(init_tf)
                    STATE.state_tf_known[grupo] = True
                logger.info(f"Init TOTAL_FAULT {grupo} = {bool(init_tf)}")
            except Exception as e:
                logger.warning(f"No se pudo leer TOTAL_FAULT inicial de {grupo}: {e}")

            handler = SubHandler(client, ncache)
            params = ua.CreateSubscriptionParameters()
            params.RequestedPublishingInterval = CFG.sampling_ms
            params.RequestedMaxKeepAliveCount = CFG.sub_keepalive_count
            params.RequestedLifetimeCount = max(CFG.sub_lifetime_count, CFG.sub_keepalive_count * 3)
            params.MaxNotificationsPerPublish = 0
            params.Priority = 0
            try:
                subscription = await client.create_subscription(params, handler)
            except TypeError:
                subscription = await client.create_subscription(CFG.sampling_ms, handler)

            cache_g["subscription"] = subscription

            # Siempre: CONTINUOUS + TOTAL_FAULT (+ TOTAL_START si corresponde)
            items = [cache_g["continuous"], cache_g["total_fault"]]
            if grupo == CFG.gate_group:
                items.append(cache_g["total_start"])
                try:
                    init_gate = await cache_g["total_start"].read_value()
                    with STATE.lock:
                        STATE.gate_open = bool(init_gate)
                        STATE.gate_known = True
                        STATE.gate_last_change = datetime.now()
                        # Rebase SOLO Dash
                        for g in CFG.node_groups:
                            STATE.ultima_marca_continuous[g] = STATE.gate_last_change
                    logger.info(f"Init {CFG.gate_group}.{CFG.gate_node} = {STATE.gate_open}")
                except Exception as e:  # pragma: no cover
                    logger.warning(f"No se pudo leer {CFG.gate_group}.{CFG.gate_node} inicial: {e}")

            await subscription.subscribe_data_change(items, queuesize=CFG.sub_queue_size)

            # GMF dinámicas: habilitar si TF=1 (independiente del gate)
            if STATE.state_tf_known.get(grupo, False) and STATE.current_total_fault.get(grupo, False):
                await ncache.enable_gmf_for_group(grupo, CFG.sub_queue_size)

            subs.append(subscription)
            logger.info(f"Suscrito grupo {grupo}: base={len(items)} (+ GMF dinámicas)")
            STATE.limpiar_error_grupo(grupo, datetime.now())
        except Exception as e:
            msg = f"Error en grupo {grupo}: {e}"
            logger.error(msg)
            STATE.marcar_error_grupo(grupo, msg)
            continue
    return subs

# --------------------------------------------------------------------------------------
# Tareas asíncronas
# --------------------------------------------------------------------------------------

async def heartbeat_telegram_y_acumulados():
    while True:
        ahora = datetime.now()
        with STATE.lock:
            for grupo in CFG.node_groups:
                # Asegurar estructuras
                STATE.acumulado_continuous.setdefault(grupo, timedelta(0))         # DASH
                STATE.ultima_marca_continuous.setdefault(grupo, ahora)             # DASH
                STATE.acumulado_continuous_tele.setdefault(grupo, timedelta(0))    # TELE
                STATE.ultima_marca_continuous_tele.setdefault(grupo, ahora)        # TELE

                cont0 = STATE.state_cont_known.get(grupo, False) and (not STATE.current_continuous.get(grupo, True))
                gate_abierto = (not STATE.gate_known) or STATE.gate_open

                # ---- DASH (visible): SOLO acumula si gate abierto y CONT=0
                if gate_abierto and cont0:
                    delta_dash = ahora - STATE.ultima_marca_continuous[grupo]
                    if delta_dash.total_seconds() > 0:
                        STATE.acumulado_continuous[grupo] += delta_dash
                STATE.ultima_marca_continuous[grupo] = ahora
                segundos_dash = int(STATE.acumulado_continuous[grupo].total_seconds())

                # ---- TELE (interno): acumula SIEMPRE que CONT=0
                if cont0:
                    delta_tele = ahora - STATE.ultima_marca_continuous_tele[grupo]
                    if delta_tele.total_seconds() > 0:
                        STATE.acumulado_continuous_tele[grupo] += delta_tele
                STATE.ultima_marca_continuous_tele[grupo] = ahora
                segundos_tele = int(STATE.acumulado_continuous_tele[grupo].total_seconds())

                # ---- Aviso "equipo en falla": solo con gate abierto, usando segundos_dash (lo visible)
                if gate_abierto and cont0 and (segundos_dash >= CFG.telegram_threshold_seconds) and (not STATE.tg_enviado.get(grupo, False)):
                    STATE.tg_enviado[grupo] = True
                    activos = [gmf_id.split(".")[1] for gmf_id in STATE.tiempos_gmf if gmf_id.startswith(grupo + ".")]
                    congeladas = [gmf_id.split(".")[1] for gmf_id in STATE.tiempos_congelados if gmf_id.startswith(grupo + ".")]
                    gmfs_para_msg = list(dict.fromkeys(activos + congeladas))
                    TELEGRAM.send(_msg_telegram(grupo, segundos_dash, gmfs_para_msg))
        await asyncio.sleep(0.2)

async def watchdog_por_grupo(client: Client, ncache: NodeCache):
    while True:
        for grupo in CFG.node_groups:
            try:
                cache_g = ncache.for_group(grupo)
                await asyncio.wait_for(cache_g["continuous"].read_value(), timeout=2.0)
                if STATE.error_grupo_activo.get(grupo, False):
                    STATE.limpiar_error_grupo(grupo, datetime.now())
            except Exception as e:
                STATE.marcar_error_grupo(grupo, f"Error en grupo {grupo}: {e}")
        await asyncio.sleep(2.0)

async def tick_oracle():
    while True:
        ahora = datetime.now()
        if (ahora - STATE.last_mon_update).total_seconds() >= 60:
            ORACLE.tick_monitoring(STATE.session_id, ahora)
            STATE.last_mon_update = ahora
        await asyncio.sleep(1.0)

# --------------------------------------------------------------------------------------
# Loop principal
# --------------------------------------------------------------------------------------

async def main():
    cargar_descripciones()
    ORACLE.ensure_schema()
    inicio_sesion = datetime.now()
    ORACLE.start_monitoring(STATE.session_id, inicio_sesion)
    STATE.last_mon_update = inicio_sesion

    while True:
        try:
            async with Client(url=CFG.url) as client:
                logger.info("Conectado al servidor OPC UA")
                if STATE.error_activo:
                    ORACLE.insert_error_conn(STATE.session_id, STATE.error_inicio, datetime.now(), STATE.ultimo_error_msg)  # type: ignore
                    STATE.error_activo = False
                    STATE.error_inicio = None
                    STATE.ultimo_error_msg = ""
                ncache = NodeCache(client).build_for_groups(CFG.node_groups)
                subs = await suscribir_grupos(client, ncache)
                tasks = [
                    asyncio.create_task(heartbeat_telegram_y_acumulados()),
                    asyncio.create_task(tick_oracle()),
                    asyncio.create_task(watchdog_por_grupo(client, ncache)),
                ]
                try:
                    await asyncio.Future()  # dormir "para siempre"
                finally:
                    for t in tasks:
                        t.cancel()
                    for s in subs:
                        with suppress(Exception):
                            await s.delete()
        except Exception as e:  # Reconexión
            if not STATE.error_activo:
                STATE.error_activo = True
                STATE.error_inicio = datetime.now()
                STATE.ultimo_error_msg = str(e)
                logger.error(f"Conexión caída: {STATE.ultimo_error_msg}")
            for grupo in CFG.node_groups:
                STATE.marcar_error_grupo(grupo, f"Conexión caída: {e}", inicio=STATE.error_inicio)
            await asyncio.sleep(2)

def thread_monitor():
    asyncio.run(main())

# --------------------------------------------------------------------------------------
# UI (Dash)
# --------------------------------------------------------------------------------------

BG = "#0f172a"
CARD = "#111827"
TEXT = "#e5e7eb"
MUTED = "#9ca3af"
ACCENT = "#22d3ee"
ACCENT_SOFT = "rgba(34,211,238,0.15)"

container_style = {
    "backgroundColor": BG,
    "minHeight": "100vh",
    "padding": "1px 22px",
    "fontFamily": "Inter, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial",
}

title_style = {"color": TEXT, "fontSize": "24px", "marginBottom": "10px", "fontWeight": 700}
card_style = {"backgroundColor": CARD, "borderRadius": "14px", "padding": "14px 16px", "boxShadow": "0 4px 16px rgba(0,0,0,0.35)", "border": f"1px solid {ACCENT_SOFT}"}
table_card_style = {**card_style, "padding": "6px 8px"}

app = Dash(__name__)
app.layout = html.Div(
    [
        dcc.Store(id="store-snapshot", storage_type="local"),
        html.Div(id="gate-banner", style={"margin": "6px 0"}),

        html.Div(
            [
                html.Div(
                    [
                        html.H3("🛠️ Fallas Activas", style=title_style),
                        html.Div(
                            [
                                dash_table.DataTable(
                                    id="tabla-fallas",
                                    columns=[
                                        {"name": "Equipo", "id": "Equipo"},
                                        {"name": "GMF", "id": "GMF"},
                                        {"name": "Descripción", "id": "Descripcion"},
                                        {"name": "Tiempo (mm:ss)", "id": "TiempoFmt"},
                                        {"name": "Tiempo OFF (seg)", "id": "TiempoOffSeg"},
                                    ],
                                    data=[],
                                    sort_action="native",
                                    page_action="native",
                                    page_size=12,
                                    style_as_list_view=True,
                                    style_table={"height": "78vh", "overflowY": "auto", "border": f"1px solid {ACCENT_SOFT}", "borderRadius": "12px"},
                                    style_header={"backgroundColor": "#111827", "color": TEXT, "fontWeight": "700", "borderBottom": f"2px solid {ACCENT_SOFT}", "position": "sticky", "top": 0, "zIndex": 1},
                                    style_cell={"backgroundColor": "#0b1220", "color": TEXT, "padding": "8px 10px", "border": "0px", "fontSize": "22px", "whiteSpace": "nowrap", "textOverflow": "ellipsis", "maxWidth": 380},
                                    style_data_conditional=[
                                        {"if": {"filter_query": "{TiempoOffSeg} >= 60"}, "backgroundColor": "#783c3c", "color": "#ffdddd", "fontWeight": "700", "borderLeft": "4px solid #ff4d4f"},
                                        {"if": {"filter_query": "{TiempoOffSeg} >= 30 && {TiempoOffSeg} < 60"}, "backgroundColor": "#3f3b1d", "color": "#fff1b8", "fontWeight": "700", "borderLeft": "4px solid #facc15"},
                                        {"if": {"row_index": "odd", "filter_query": "{TiempoOffSeg} < 30"}, "backgroundColor": "#0d1424"},
                                        {"if": {"state": "active"}, "border": f"1px solid {ACCENT_SOFT}"},
                                        {"if": {"column_id": "TiempoFmt"}, "fontWeight": "700", "color": ACCENT},
                                    ],
                                ),
                                html.Div(id="last-update", style={"color": MUTED, "fontSize": "12px", "marginTop": "8px"}),
                            ],
                            style=table_card_style,
                        ),
                    ],
                    style={"flexBasis": "75%", "minWidth": 0},
                ),
                html.Div(
                    [
                        html.H3("🚨 Errores de conexión", style=title_style),
                        html.Div(
                            [
                                dash_table.DataTable(
                                    id="tabla-errores",
                                    columns=[
                                        {"name": "Grupo", "id": "Grupo"},
                                        {"name": "Desde", "id": "Desde"},
                                        {"name": "Hace", "id": "Hace"},
                                        {"name": "Msj", "id": "Mensaje"},
                                    ],
                                    data=[],
                                    sort_action="native",
                                    page_action="native",
                                    page_size=8,
                                    style_as_list_view=True,
                                    style_table={"height": "78vh", "overflowY": "auto", "border": f"1px solid {ACCENT_SOFT}", "borderRadius": "12px"},
                                    style_header={"backgroundColor": "#111827", "color": TEXT, "fontWeight": "700", "borderBottom": f"2px solid {ACCENT_SOFT}", "position": "sticky", "top": 0, "zIndex": 1},
                                    style_cell={"backgroundColor": "#0b1220", "color": TEXT, "padding": "8px 10px", "border": "0px", "fontSize": "18px", "whiteSpace": "nowrap", "textOverflow": "ellipsis", "maxWidth": 600},
                                    style_data_conditional=[
                                        {"if": {"row_index": "odd"}, "backgroundColor": "#0d1424"},
                                        {"if": {"state": "active"}, "border": f"1px solid {ACCENT_SOFT}"},
                                        {"if": {"column_id": "Hace"}, "fontWeight": "700", "color": ACCENT},
                                        {"if": {"filter_query": "{Mensaje} contains 'Error' || {Mensaje} contains 'Failed'"}, "backgroundColor": "#5b1a1a", "color": "#ffe5e5", "borderLeft": "4px solid #ef4444", "fontWeight": "700"},
                                    ],
                                ),
                            ],
                            style=table_card_style,
                        ),
                    ],
                    style={"flexBasis": "25%", "minWidth": 0},
                ),
            ],
            style={"display": "flex", "flexDirection": "row", "gap": "16px", "alignItems": "stretch", "width": "100%"},
        ),
        dcc.Interval(id="intervalo", interval=CFG.ui_interval_ms, n_intervals=0),
    ],
    style=container_style,
)

@app.callback(Output("store-snapshot", "data"), Input("intervalo", "n_intervals"))
def calcular_snapshot(_):
    ahora = datetime.now()
    filas_fallas, filas_errores = [], []
    with STATE.lock:
        for gmf_id, inicio in STATE.tiempos_gmf.items():
            equipo, gmf = gmf_id.split(".")
            segundos = int((ahora - inicio).total_seconds())
            tiempo_off_seg = int(STATE.acumulado_continuous.get(equipo, timedelta()).total_seconds())
            filas_fallas.append({"Equipo": equipo, "GMF": gmf, "Descripcion": _desc_gmf(equipo, gmf), "TiempoFmt": _fmt_mmss(segundos), "TiempoOffSeg": tiempo_off_seg})
        for gmf_id, seg_congelado in STATE.tiempos_congelados.items():
            if gmf_id not in STATE.tiempos_gmf:
                equipo, gmf = gmf_id.split(".")
                tiempo_off_seg = int(STATE.acumulado_continuous.get(equipo, timedelta()).total_seconds())
                filas_fallas.append({"Equipo": equipo, "GMF": gmf, "Descripcion": _desc_gmf(equipo, gmf), "TiempoFmt": _fmt_mmss(seg_congelado), "TiempoOffSeg": tiempo_off_seg})
        for grupo in CFG.node_groups:
            if STATE.error_grupo_activo.get(grupo):
                ini = STATE.error_grupo_inicio.get(grupo) or ahora
                segundos = max(0, int((ahora - ini).total_seconds()))
                filas_errores.append({"Grupo": grupo, "Desde": ini.strftime("%Y-%m-%d %H:%M:%S"), "Hace": _fmt_mmss(segundos), "Mensaje": STATE.error_grupo_msg.get(grupo, "Error de conexión")})
        if STATE.error_activo and STATE.error_inicio:
            segs_g = max(0, int((ahora - STATE.error_inicio).total_seconds()))
            filas_errores.insert(0, {"Grupo": "GLOBAL", "Desde": STATE.error_inicio.strftime("%Y-%m-%d %H:%M:%S"), "Hace": _fmt_mmss(segs_g), "Mensaje": STATE.ultimo_error_msg or "Error de conexión"})
    return {
        "fallas": filas_fallas,
        "errores": filas_errores,
        "last": f"Última actualización: {ahora.strftime('%H:%M:%S')}",
        "gate": {
            "open": STATE.gate_open,
            "since": STATE.gate_last_change.strftime("%Y-%m-%d %H:%M:%S") if STATE.gate_last_change else "",
            "text": (
                (f"▶️ TOTAL_START ({CFG.gate_group}) : ON — desde {_fmt_mmss(int((ahora - STATE.gate_last_change).total_seconds()))}"
                 if STATE.gate_open else
                 f"🧊 TOTAL_START ({CFG.gate_group}) : OFF — desde {_fmt_mmss(int((ahora - STATE.gate_last_change).total_seconds()))}")
            ) if STATE.gate_known and STATE.gate_last_change else "⏯ TOTAL_START: (desconocido)",
        }
    }

@app.callback(Output("tabla-fallas", "data"),
              Output("tabla-errores", "data"),
              Output("last-update", "children"),
              Output("gate-banner", "children"),
              Output("gate-banner", "style"),
              Output("tabla-fallas", "style_table"),
              Output("tabla-errores", "style_table"),
              Input("store-snapshot", "data"))
def renderizar_tablas(store_data):
    if not store_data:
        return [], [], "", "", {}, {}, {}

    gate = store_data.get("gate", {})
    gate_text = gate.get("text", "⏯ TOTAL_START: (desconocido)")
    gate_open = bool(gate.get("open", False))

    base_banner = {
        "display": "inline-block",
        "padding": "8px 12px",
        "borderRadius": "10px",
        "fontWeight": "700",
        "border": f"1px solid {ACCENT_SOFT}",
    }
    if gate_open:
        gate_style = {**base_banner, "backgroundColor": "#103013", "color": "#dcfce7", "borderLeft": "4px solid #22c55e"}
    else:
        gate_style = {**base_banner, "backgroundColor": "#5b1a1a", "color": "#ffe5e5", "borderLeft": "4px solid #ef4444"}

    base_table_style = {
        "height": "78vh",
        "overflowY": "auto",
        "border": f"1px solid {ACCENT_SOFT}",
        "borderRadius": "12px",
        "transition": "opacity 200ms ease",
    }
    style_fallas  = {**base_table_style, "opacity": (1.0 if gate_open else 0.45)}
    style_errores = {**base_table_style, "opacity": (1.0 if gate_open else 0.45)}

    return (
        store_data.get("fallas", []),
        store_data.get("errores", []),
        store_data.get("last", ""),
        gate_text,
        gate_style,
        style_fallas,
        style_errores,
    )

# --------------------------------------------------------------------------------------
# WSGI / Waitress
# --------------------------------------------------------------------------------------

server = app.server

def start_monitor_once():
    if not getattr(start_monitor_once, "_started", False):
        monitor_thread = threading.Thread(target=thread_monitor, daemon=True)
        monitor_thread.start()
        start_monitor_once._started = True
        logger.info("Monitor thread iniciado (Windows/Waitress)")

start_monitor_once()

@server.route("/healthz")
def healthz():
    return Response("ok", status=200, mimetype="text/plain")

if __name__ == "__main__":
    from waitress import serve  # type: ignore
    logger.info(f"Sirviendo con Waitress en http://{CFG.host}:{CFG.port}")
    serve(server, host=CFG.host, port=CFG.port)
