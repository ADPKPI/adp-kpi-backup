from datetime import timedelta
from commands import *
import os
from config import master_servers
import logging


def rotate_backups(backup_directory, max_hourly_backups=6, max_daily_backups=30):
    """
    Ротує резервні копії у вказаному каталозі, видаляючи старі резервні копії та зберігаючи нові.

    Аргументи:
        backup_directory (str): Шлях до каталогу з резервними копіями.
        max_hourly_backups (int, optional): Максимальна кількість годинних резервних копій, що зберігаються. За замовчуванням 6.
        max_daily_backups (int, optional): Максимальна кількість щоденних резервних копій, що зберігаються. За замовчуванням 30.

    """
    all_backups = sorted(os.listdir(backup_directory), reverse=True)
    now = datetime.now()

    if now.day == 1:
        last_month = (now - timedelta(days=1)).strftime('%Y-%m')
        monthly_backup_filename = f"monthly_backup_{last_month}.sql"
        daily_backups = [b for b in all_backups if 'daily_backup' in b]
        if daily_backups:
            os.rename(os.path.join(backup_directory, daily_backups[0]), os.path.join(backup_directory, monthly_backup_filename))
            logging.info(f"Renamed {daily_backups[0]} to monthly backup: {monthly_backup_filename}")
            all_backups = [monthly_backup_filename] + all_backups[1:]

    for backup in all_backups[max_hourly_backups:]:
        if 'hourly_backup' in backup:
            os.remove(os.path.join(backup_directory, backup))
            logging.info(f"Deleted old hourly backup: {backup}")

    daily_backups = [b for b in all_backups if 'daily_backup' in b]
    for backup in daily_backups[max_daily_backups:]:
        os.remove(os.path.join(backup_directory, backup))
        logging.info(f"Deleted old daily backup: {backup}")

    today_date = now.strftime('%Y-%m-%d')
    daily_backup_filename = f"{backup_directory}/daily_backup_{today_date}.sql"

    if not os.path.exists(daily_backup_filename):
        if all_backups:
            os.rename(os.path.join(backup_directory, all_backups[0]), daily_backup_filename)
            logging.info(f"Renamed {all_backups[0]} to daily backup: {daily_backup_filename}")
            all_backups[0] = daily_backup_filename.split('/')[-1]


def main(operation, server_name=''):
    """
    Головна функція для виконання операцій з резервними копіями.

    Аргументи:
        operation (str): Операція для виконання ('rotate' або 'backup').
        server_name (str, optional): Назва сервера для виконання операції резервного копіювання. За замовчуванням ''.

    """
    if operation == 'rotate':
        for server in master_servers:
            rotate_backups(backup_directory=f"{backup_path}/{server['name']}",
                           max_hourly_backups=6, max_daily_backups=30)
    elif operation == 'backup':
        for server in master_servers:
            if server_name == server['name'] or server_name == '':
                create_db_dump(ssh_host=server['ssh_host'],
                               ssh_user=server['ssh_user'],
                               ssh_key_path=server['ssh_key_path'],
                               db_name='adp_pizza_db',
                               server_name=server['name'],
                               path=f"/backups/{server['name']}")

if __name__ == "__main__":
    import sys
    operation = sys.argv[1]  # 'rotate' або 'backup'
    server_name = sys.argv[2] if len(sys.argv) > 2 else ''
    main(operation, server_name)
