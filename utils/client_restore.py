from __future__ import annotations

import os
import logging
import shlex
import subprocess
import argparse
import sys
from datetime import datetime
from typing import Optional, List, Dict

from .client_backup import docker_exec, run, load_config
from .logging_config import init_logging

LOGGER = logging.getLogger(__name__)

POSTGRES_EXT_PRIORIDAD = [".backup", ".sql"]  # prefer custom format
MYSQL_EXT = [".sql"]


def _list_backup_files(backup_dir: str, db_name: str, exts: List[str]) -> List[str]:
	if not os.path.isdir(backup_dir):
		return []
	prefix = f"{db_name}_"
	files = []
	for f in os.listdir(backup_dir):
		if not f.startswith(prefix):
			continue
		if any(f.endswith(ext) for ext in exts):
			files.append(os.path.join(backup_dir, f))
	# sort by mtime desc
	files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
	return files


def _choose_latest(files: List[str], preferred_order: List[str]) -> Optional[str]:
	if not files:
		return None
	# group by extension preference
	for ext in preferred_order:
		for f in files:
			if f.endswith(ext):
				return f
	return files[0]


def _restore_postgres(db_name: str, file_path: str, user: str, password: str, port: str, host: str, *, container: Optional[str]) -> None:
	env_vars = {"PGPASSWORD": password}
	ext = os.path.splitext(file_path)[1]
	if container:
		remote = f"/tmp/restore_{os.path.basename(file_path)}"
		# copiar al contenedor
		run(["docker", "cp", file_path, f"{container}:{remote}"])
		try:
			if ext == ".backup":  # formato custom
				# -C crea la DB, limpiamos antes por si existe
				cmd = ["pg_restore", "-U", user, "-h", host, "-p", port, "-C", "-d", "postgres", remote]
				docker_exec(container, cmd, env_vars=env_vars)
			else:  # .sql
				# Drop + create y luego cargar el script
				stmt = f"DROP DATABASE IF EXISTS {shlex.quote(db_name)}; CREATE DATABASE {shlex.quote(db_name)};"
				docker_exec(container, ["psql", "-U", user, "-h", host, "-p", port, "-d", "postgres", "-c", stmt], env_vars=env_vars)
				docker_exec(container, ["psql", "-U", user, "-h", host, "-p", port, "-d", db_name, "-f", remote], env_vars=env_vars)
		finally:
			# limpiar
			try:
				docker_exec(container, ["rm", "-f", remote])
			except Exception:  # noqa
				pass
	else:
		env = {**os.environ, **env_vars}
		if ext == ".backup":
			# crear DB (pg_restore -C la crea; para eso apuntamos a postgres)
			run(["pg_restore", "-U", user, "-h", host, "-p", port, "-C", "-d", "postgres", file_path], env=env)
		else:
			stmt = f"DROP DATABASE IF EXISTS {shlex.quote(db_name)}; CREATE DATABASE {shlex.quote(db_name)};"
			run(["psql", "-U", user, "-h", host, "-p", port, "-d", "postgres", "-c", stmt], env=env)
			run(["psql", "-U", user, "-h", host, "-p", port, "-d", db_name, "-f", file_path], env=env)


def _restore_mysql(db_name: str, file_path: str, user: str, password: str, port: str, host: str, *, container: Optional[str]) -> None:
	# construimos comandos de drop/create + carga
	pre_sql = f"DROP DATABASE IF EXISTS `{db_name}`; CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
	drop_create_cmd = ["mysql", f"-u{user}", f"-p{password}", f"-P{port}", "-e", pre_sql]
	if host:
		drop_create_cmd.insert(3, f"-h{host}")

	if container:
		docker_exec(container, drop_create_cmd)
		# restore
		# copiamos archivo al contenedor para evitar problemas de redirección con docker exec
		remote = f"/tmp/restore_{os.path.basename(file_path)}"
		run(["docker", "cp", file_path, f"{container}:{remote}"])
		try:
			restore_cmd = f"mysql -u{shlex.quote(user)} -p'{password}' -P{shlex.quote(port)}"
			if host:
				restore_cmd += f" -h{shlex.quote(host)}"
			restore_cmd += f" {shlex.quote(db_name)} < {shlex.quote(remote)}"
			docker_exec(container, ["sh", "-c", restore_cmd])
		finally:
			try:
				docker_exec(container, ["rm", "-f", remote])
			except Exception:  # noqa
				pass
	else:
		subprocess.run(drop_create_cmd, check=True)
		restore_cmd = ["bash", "-c", _mysql_restore_shell(user, password, port, host, db_name, file_path)]
		subprocess.run(restore_cmd, check=True)


def _mysql_restore_shell(user: str, password: str, port: str, host: str, db: str, path: str) -> str:
	parts = [f"mysql -u{shlex.quote(user)} -p'{password}' -P{shlex.quote(port)}"]
	if host:
		parts.append(f"-h{shlex.quote(host)}")
	parts.append(shlex.quote(db))
	return " ".join(parts) + f" < {shlex.quote(path)}"


def restore_database(db_conf: Dict[str, str], *, db: Optional[str] = None, file_path: Optional[str] = None) -> Optional[str]:
	"""Restore a database.

	Args:
		db_conf: dict entry from configuration (must include type, username, password, backup_dir, etc.)
		db: database name (optional) overrides db_conf['db']
		file_path: explicit backup file path; if omitted selects latest for db.

	Returns path of backup file used or None if failed.
	"""
	init_logging()  # ensure logging configured
	db_type = db_conf.get('type', 'postgres').lower()
	user = db_conf['username']
	password = db_conf['password']
	port = db_conf.get('port', '5432' if db_type == 'postgres' else '3306')
	host = db_conf.get('host', '127.0.0.1')
	container = db_conf.get('container')
	backup_dir = db_conf.get('backup_dir', './')
	target_db = db or db_conf.get('db')
	if not target_db and not file_path:
		LOGGER.error("Debe especificar 'db' o 'file_path' para restaurar.")
		return None
	# Derivar db desde file si no se pasó
	if not target_db and file_path:
		base = os.path.basename(file_path)
		# patron esperado: <dbname>_YYYYmmdd_HHMMSS.ext
		target_db = base.split('_', 1)[0]

	try:
		if not file_path:
			if db_type == 'postgres':
				files = _list_backup_files(backup_dir, target_db, POSTGRES_EXT_PRIORIDAD)
				file_path = _choose_latest(files, POSTGRES_EXT_PRIORIDAD)
			elif db_type in ('mysql', 'mariadb'):
				files = _list_backup_files(backup_dir, target_db, MYSQL_EXT)
				file_path = _choose_latest(files, MYSQL_EXT)
			else:
				LOGGER.error("Tipo de base de datos no soportado: %s", db_type)
				return None
		if not file_path:
			LOGGER.error("No se encontraron backups para %s en %s", target_db, backup_dir)
			return None
		LOGGER.info("Restaurando %s desde %s", target_db, file_path)
		if db_type == 'postgres':
			_restore_postgres(target_db, file_path, user, password, port, host, container=container)
		else:
			_restore_mysql(target_db, file_path, user, password, port, host, container=container)
		LOGGER.info("Restauración completada: %s", target_db)
		return file_path
	except subprocess.CalledProcessError as e:
		safe_conf = {k: ('***' if k == 'password' else v) for k, v in db_conf.items()}
		stderr = getattr(e, 'stderr', b'')
		if isinstance(stderr, bytes):
			stderr = stderr.decode(errors='ignore')
		LOGGER.error("Fallo restauración (%s): rc=%s cmd=%s stderr=%s", safe_conf, e.returncode, e.cmd, stderr.strip())
	except Exception:
		LOGGER.exception("Error inesperado durante la restauración")
	return None


__all__ = [
	'restore_database'
]

# ---------------- CLI -----------------


def _human_size(path: str) -> str:
	try:
		size = os.path.getsize(path)
	except OSError:
		return '?'  # file vanished
	for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
		if size < 1024:
			return f"{size:.1f}{unit}"
		size /= 1024
	return f"{size:.1f}PB"


def _select_config(config: list, *, db_type: str, container: Optional[str]) -> Optional[Dict[str, str]]:
	matches = [c for c in config if c.get('type', 'postgres').lower() == db_type]
	if container:
		matches = [c for c in matches if c.get('container') == container]
	if not matches:
		LOGGER.error("No hay configuraciones que coincidan con type=%s container=%s", db_type, container)
		return None
	if len(matches) > 1:
		LOGGER.error("Multiples configuraciones coinciden. Especifique --container. Coincidencias: %s", matches)
		return None
	return matches[0]


def cli(argv: Optional[list] = None) -> int:
	parser = argparse.ArgumentParser(description="Restaurar bases de datos desde backups generados por client_backup")
	sub = parser.add_subparsers(dest='command', required=True)

	common = argparse.ArgumentParser(add_help=False)
	common.add_argument('--config', default=os.path.join(os.path.dirname(__file__), '../databases.json'), help='Ruta al databases.json')
	common.add_argument('--type', required=True, choices=['postgres', 'mysql', 'mariadb'], help='Tipo de motor')
	common.add_argument('--container', help='Filtrar por nombre de contenedor si hay varias entradas')
	common.add_argument('--db', help='Nombre de la base (si se omite se intenta deducir de --file en restore)')

	p_list = sub.add_parser('list', parents=[common], help='Listar backups disponibles para una base')
	p_list.add_argument('--backup-dir', help='Sobrescribe backup_dir de la config')

	p_res = sub.add_parser('restore', parents=[common], help='Restaurar una base')
	p_res.add_argument('--file', help='Ruta específica al backup (si falta se selecciona el último)')
	p_res.add_argument('--backup-dir', help='Sobrescribe backup_dir de la config')

	args = parser.parse_args(argv)
	init_logging()

	config = load_config(args.config)
	db_conf = _select_config(config, db_type=args.type, container=args.container)
	if not db_conf:
		return 2
	if args.backup_dir:
		db_conf = {**db_conf, 'backup_dir': args.backup_dir}

	if args.command == 'list':
		target_db = args.db or db_conf.get('db')
		if not target_db:
			LOGGER.error("--db requerido para listar si la config no tiene 'db'")
			return 2
		exts = POSTGRES_EXT_PRIORIDAD if args.type == 'postgres' else MYSQL_EXT
		files = _list_backup_files(db_conf.get('backup_dir', './'), target_db, exts)
		if not files:
			print("(sin backups)")
			return 0
		for f in files:
			ts = datetime.fromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d %H:%M:%S')
			print(f"{ts}\t{_human_size(f)}\t{f}")
		return 0
	elif args.command == 'restore':
		used = restore_database(db_conf, db=args.db, file_path=args.file)
		return 0 if used else 1
	return 0


def main():  # entry point
	sys.exit(cli())


if __name__ == '__main__':
	main()


