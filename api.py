from flask import Flask, request, jsonify
from commands import *
from config import *

app = Flask(__name__)

@app.route('/create_db_dump', methods=['POST'])
def api_create_db_dump():
    """
    API кінцева точка для створення дампу бази даних.

    Приймає:
        id (int): Ідентифікатор сервера.

    Повертає:
        json: Результат виконання створення дампу бази даних.
    """
    id = int(request.args.get('id'))
    result = create_db_dump(master_servers[id]['ssh_host'], master_servers[id]['ssh_user'], ssh_private_key_path, 'adp_pizza_db', master_servers[id]['name'], backup_path)
    return jsonify(result), 200

@app.route('/list_backups', methods=['GET'])
def api_list_backups():
    """
    API кінцева точка для перелічення наявних резервних копій.

    Приймає:
        id (int): Ідентифікатор сервера.

    Повертає:
        json: Список наявних резервних копій.
    """
    id = int(request.args.get('id'))
    result = list_backups(path = f"{backup_path}/{master_servers[id]['name']}")
    return jsonify(result), 200

@app.route('/upload_backup', methods=['POST'])
def api_upload_backup():
    """
    API кінцева точка для завантаження резервної копії на сервери.

    Приймає:
        id (int): Ідентифікатор сервера.
        filename (str): Назва файлу резервної копії.

    Повертає:
        json: Результат виконання завантаження резервної копії.
    """
    id = int(request.args.get('id'))
    filename = request.args.get('filename')
    result = upload_backup(filename, backup_path, master_servers[id]['name'], servers, ssh_private_key_path)
    return jsonify(result), 200

if __name__ == '__main__':
    app.run(host='78.140.162.131', port=5000)
