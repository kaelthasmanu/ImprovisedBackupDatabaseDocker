from __future__ import annotations

import os
import json
import shlex
import logging
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

CONFIG_FILE = os.path.join(os.path.dirname(__file__), '../databases.json')
POSTGRES_EXCLUDE = {"postgres"}
MYSQL_EXCLUDE = {"information_schema", "performance_schema", "mysql", "sys"}

def load_config(path: str = CONFIG_FILE) -> List[Dict[str, Any]]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('databases', []) or []
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error("Config error (%s): %s", path, e)
        return []

DATABASES: List[Dict[str, Any]] = load_config()

def ensure_backup_dir(backup_dir: str) -> None:
    os.makedirs(backup_dir, exist_ok=True)

def run(cmd: List[str], *, env: Optional[Dict[str, str]] = None, capture: bool = False) -> subprocess.CompletedProcess:
    logging.debug("RUN: %s", ' '.join(shlex.quote(c) for c in cmd))
    return subprocess.run(cmd, check=True, text=capture, capture_output=capture, env=env)

def docker_exec(container: str, inner_cmd: List[str], env_vars: Optional[Dict[str, str]] = None, capture: bool = False) -> subprocess.CompletedProcess:
    cmd = ["docker", "exec"]
    if env_vars:
        for k, v in env_vars.items():
            cmd += ["-e", f"{k}={v}"]
    cmd.append(container)
    cmd += inner_cmd
    return run(cmd, capture=capture)

def list_postgres_databases(user: str, password: str, port: str, host: str, *, container: Optional[str]) -> List[str]:
    query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    if container:
        # Versión simplificada para contenedores - sin host/port
        base_cmd = ["psql", "-U", user, "-d", "postgres", "-t", "-c", query]
        result = docker_exec(container, base_cmd, capture=True)
    else:
        # Versión original con contraseña para conexiones externas
        base_cmd = ["psql", "-U", user, "-h", host, "-p", port, "-d", "postgres", "-t", "-c", query]
        result = run(base_cmd, env={**os.environ, "PGPASSWORD": password}, capture=True)
    return [d.strip() for d in result.stdout.splitlines() if d.strip() and d.strip() not in POSTGRES_EXCLUDE]

def list_mysql_databases(user: str, password: str, port: str, host: str, *, container: Optional[str]) -> List[str]:
    query = "SHOW DATABASES;"
    if container:
        # Versión simplificada para contenedores - sin host/port
        base_parts = ["mysql", f"-u{user}", f"-p{password}", "-e", query]
    else:
        base_parts = ["mysql", f"-u{user}", f"-p{password}", f"-P{port}", "-e", query]
        if host:
            base_parts.insert(3, f"-h{host}")  # after user/pass

    def attempt(parts: List[str]) -> subprocess.CompletedProcess:
        if container:
            return docker_exec(container, parts, capture=True)
        return run(parts, capture=True)

    try:
        result = attempt(base_parts)
    except subprocess.CalledProcessError as e:
        logging.warning("MySQL SHOW DATABASES failed (%s). Retrying without host...", e.returncode)
        # remove any -hhost occurrences
        retry = [p for p in base_parts if not (p.startswith('-h') and p != '-h')]
        try:
            result = attempt(retry)
        except subprocess.CalledProcessError:
            raise
    return [d.strip() for d in result.stdout.splitlines() if d.strip() and d.strip() not in MYSQL_EXCLUDE and not d.startswith("Database")]

def backup_postgres_db(db: str, user: str, password: str, port: str, host: str, backup_dir: str, container: Optional[str]) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    r_plain = f"/tmp/{db}_{ts}.sql"
    r_custom = f"/tmp/{db}_{ts}.backup"
    l_plain = os.path.join(backup_dir, f"{db}_{ts}.sql")
    l_custom = os.path.join(backup_dir, f"{db}_{ts}.backup")
    if container:
        # Versión simplificada para contenedores - sin host/port
        docker_exec(container, ["pg_dump", "-U", user, "-d", db, "-F", "p", "-f", r_plain])
        run(["docker", "cp", f"{container}:{r_plain}", l_plain])
        docker_exec(container, ["rm", r_plain])
        docker_exec(container, ["pg_dump", "-U", user, "-d", db, "-F", "c", "-f", r_custom])
        run(["docker", "cp", f"{container}:{r_custom}", l_custom])
        docker_exec(container, ["rm", r_custom])
    else:
        env = {**os.environ, "PGPASSWORD": password}
        run(["pg_dump", "-U", user, "-h", host, "-p", port, "-d", db, "-F", "p", "-f", l_plain], env=env)
        run(["pg_dump", "-U", user, "-h", host, "-p", port, "-d", db, "-F", "c", "-f", l_custom], env=env)

def backup_mysql_db(db: str, user: str, password: str, port: str, host: str, backup_dir: str, container: Optional[str]) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    l_plain = os.path.join(backup_dir, f"{db}_{ts}.sql")
    r_plain = f"/tmp/{db}_{ts}.sql"
    if container:
        # Versión simplificada para contenedores - sin host/port
        mysqldump_cmd = f"mysqldump -u{shlex.quote(user)} -p'{password}' {shlex.quote(db)} > {shlex.quote(r_plain)}"
    else:
        mysqldump_cmd = f"mysqldump -u{shlex.quote(user)} -p'{password}' -P{shlex.quote(port)}"
        if host:
            mysqldump_cmd += f" -h{shlex.quote(host)}"
        mysqldump_cmd += f" {shlex.quote(db)} > {shlex.quote(r_plain)}"
    
    if container:
        docker_exec(container, ["sh", "-c", mysqldump_cmd])
        run(["docker", "cp", f"{container}:{r_plain}", l_plain])
        docker_exec(container, ["rm", r_plain])
    else:
        with open(l_plain, 'w', encoding='utf-8') as f:
            subprocess.run(["bash", "-c", mysqldump_cmd.replace(f"> {shlex.quote(r_plain)}", "")], check=True, stdout=f)

def backup_database(db_conf: Dict[str, Any]) -> None:
    db_type = db_conf.get('type', 'postgres').lower()
    user = db_conf['username']
    password = db_conf['password']
    port = db_conf.get('port', '5432' if db_type == 'postgres' else '3306')
    host = db_conf.get('host', '127.0.0.1')
    backup_dir = db_conf.get('backup_dir', './')
    container = db_conf.get('container')
    ensure_backup_dir(backup_dir)

    try:
        if db_type == 'postgres':
            dbs = [db_conf['db']] if db_conf.get('db') else list_postgres_databases(user, password, port, host, container=container)
            for db in dbs:
                logging.info("Backing up Postgres DB: %s", db)
                backup_postgres_db(db, user, password, port, host, backup_dir, container)
        elif db_type in ('mysql', 'mariadb'):
            dbs = [db_conf['db']] if db_conf.get('db') else list_mysql_databases(user, password, port, host, container=container)
            for db in dbs:
                logging.info("Backing up %s DB: %s", db_type, db)
                backup_mysql_db(db, user, password, port, host, backup_dir, container)
        else:
            logging.error("Unsupported DB type: %s", db_type)
    except subprocess.CalledProcessError as e:
        safe_conf = {k: ('***' if k == 'password' else v) for k, v in db_conf.items()}
        stderr = getattr(e, 'stderr', b'')
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors='ignore')
        logging.error("Backup failed (%s): rc=%s cmd=%s stderr=%s", safe_conf, e.returncode, e.cmd, stderr.strip())

def backup_all() -> None:
    for conf in DATABASES:
        backup_database(conf)

__all__ = [
    'DATABASES', 'backup_database', 'backup_all'
]
