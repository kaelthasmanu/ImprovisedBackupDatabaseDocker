import logging
import os
import time
from datetime import datetime
from typing import Optional
from utils.client_backup import backup_database, DATABASES
from utils.logging_config import init_logging

""" Commands CLi:
     python3 -m utils.client_restore list --type postgres --db squidstasts --container my_postgres 
      python3 -m utils.client_restore restore --type postgres --db squidstasts --container my_postgres
      python3 -m utils.client_restore restore --type postgres --file ./backups/postgres/squidstasts_20250807_230117.backup --container my_postgres
      python3 -m utils.client_restore list --type mysql --db midb --container my_mysql python3 -m utils.client_restore restore --type mysql --db midb --container my_mysql
 """

def _has_today_backup(backup_dir: str, db_name: Optional[str], *, extensions=(".sql", ".backup")) -> bool:
    if not os.path.isdir(backup_dir):
        return False
    date_tag = datetime.now().strftime("%Y%m%d")
    for f in os.listdir(backup_dir):
        if db_name:
            if not f.startswith(f"{db_name}_{date_tag}"):
                continue
        else:
            # buscar patrón _YYYYMMDD_ en el nombre
            if f"_{date_tag}_" not in f:
                continue
        if any(f.endswith(ext) for ext in extensions):
            return True
    return False


def run_once():
    for db_conf in DATABASES:
        db_type = db_conf.get('type', 'postgres').lower()
        backup_dir = db_conf.get('backup_dir', './')
        named_db = db_conf.get('db')
        if _has_today_backup(backup_dir, named_db):
            if named_db:
                logging.info("Ya existe respaldo de hoy para %s (%s)", named_db, db_type)
            else:
                logging.info("Ya existe respaldo de hoy (config múltiple %s) en %s", db_type, backup_dir)
            continue
        logging.info("Respaldando base(s) (%s): %s", db_type, {k: ('***' if k == 'password' else v) for k, v in db_conf.items()})
        backup_database(db_conf)


def daemon(interval_seconds: int = 600):
    init_logging()
    logging.info("Iniciando daemon de respaldo (intervalo=%ss)", interval_seconds)
    while True:
        try:
            run_once()
            logging.info("Verificación completada. Próxima en %s segundos", interval_seconds)
        except Exception:
            logging.exception("Error durante ciclo de respaldo")
        time.sleep(interval_seconds)


def main():
    # Si se quiere solo un ciclo inmediato, llamar a run_once(); para modo daemon usar daemon().
    daemon(60)  # 10 minutos

if __name__ == "__main__":
    main()
