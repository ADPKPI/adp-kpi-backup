from paramiko import SSHClient, AutoAddPolicy
import paramiko
from scp import SCPClient
from datetime import datetime
import os
from config import *
import logging


def create_db_dump(ssh_host, ssh_user, ssh_key_path, db_name, server_name, path):
    """
    Створює дамп бази даних на віддаленому сервері та зберігає його на локальному сервері резервного копіювання.

    Аргументи:
        ssh_host (str): Ім'я хоста або IP-адреса SSH-сервера.
        ssh_user (str): Ім'я користувача для підключення через SSH.
        ssh_key_path (str): Шлях до файлу приватного ключа SSH.
        db_name (str): Назва бази даних для створення дампа.
        server_name (str): Назва сервера для іменування файлу дампа.
        path (str): Шлях до каталогу для збереження файлу дампа.

    Повертає:
        dict: Словник, що містить повідомлення про процес створення дампа.
    """
    response = {'messages': []}
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    dump_file_name = f'{server_name}-backup-{current_time}.sql'
    ssh_key_path = os.path.expanduser(ssh_key_path)

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ssh_host, username=ssh_user, key_filename=ssh_key_path)

        dump_command = f"mysqldump -u {ssh_user} {db_name} > {path}/{dump_file_name}"
        stdin, stdout, stderr = ssh.exec_command(dump_command)
        stderr = stderr.read().decode()
        if stderr:
            logging.error(stderr)
            response['messages'].append(f"Помилка: {stderr}")
        else:
            success_msg = f"Дамп успішно створено: {path}/{dump_file_name}"
            logging.info(success_msg)
            response['messages'].append(success_msg)

            with SCPClient(ssh.get_transport()) as scp:
                scp.get(f'{path}/{dump_file_name}', local_path=f'{path}/{server_name}/{dump_file_name}')
                backup_msg = f"Резервну копію скопійовано на сервер резервного копіювання: {path}/{server_name}/{dump_file_name}"
                logging.info(backup_msg)
                response['messages'].append(backup_msg)

            delete_command = f"rm {path}/{dump_file_name}"
            ssh.exec_command(delete_command)
            delete_msg = f"Оригінальний файл резервної копії видалено: {path}/{dump_file_name}"
            logging.info(delete_msg)
            response['messages'].append(delete_msg)

        ssh.close()
        return response
    except Exception as e:
        error_msg = f"Виникла помилка: {str(e)}"
        logging.error(error_msg)
        response['messages'].append(error_msg)
        return response


def list_backups(path):
    """
    Перелічує наявні резервні копії у вказаному каталозі.

    Аргументи:
        path (str): Шлях до каталогу, де знаходяться резервні копії.

    Повертає:
        dict: Словник, що містить повідомлення та список резервних копій.
    """
    response = {'messages': [], 'backups': []}
    try:
        backups_list = os.listdir(path)
        if backups_list:
            response['messages'].append("Доступні резервні копії знайдено.")
            response['backups'] = backups_list
            logging.info("Резервні копії успішно знайдено.")
        else:
            response['messages'].append("Резервні копії не знайдено.")
            logging.info("Резервні копії не знайдено.")
    except Exception as e:
        error_msg = f"Помилка доступу до каталогу резервних копій: {str(e)}"
        logging.error(error_msg)
        response['messages'].append(error_msg)
    return response


def upload_backup(backup_file_name, path, server_name, servers, ssh_private_key_path):
    """
    Завантажує резервну копію на вказані сервери.

    Аргументи:
        backup_file_name (str): Назва файлу резервної копії.
        path (str): Шлях до каталогу, де знаходиться резервна копія.
        server_name (str): Назва сервера для іменування файлу резервної копії.
        servers (list): Список серверів для завантаження резервної копії.
        ssh_private_key_path (str): Шлях до файлу приватного ключа SSH.

    Повертає:
        dict: Словник, що містить повідомлення про процес завантаження.
    """
    response = {'messages': []}
    for server in servers:
        try:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(server['ssh_host'], username=server['ssh_user'], key_filename=os.path.expanduser(ssh_private_key_path))

            with SCPClient(ssh.get_transport()) as scp:
                scp.put(f'{path}/{server_name}/{backup_file_name}', f'{path}/{backup_file_name}')
                success_msg = f"Резервну копію {backup_file_name} завантажено на {server['ssh_host']}: {path}/{backup_file_name}"
                response['messages'].append(success_msg)
                logging.info(success_msg)
            ssh.close()
        except Exception as e:
            error_msg = f"Не вдалося завантажити {backup_file_name} на {server['ssh_host']}: {str(e)}"
            response['messages'].append(error_msg)
            logging.error(error_msg)
    return response
