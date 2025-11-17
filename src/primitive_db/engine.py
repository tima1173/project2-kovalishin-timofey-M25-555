# src/primitive_db/engine.py
import shlex
import prompt

from primitive_db.core import create_table, drop_table
from primitive_db.utils import load_metadata, save_metadata

def print_help():
    """Prints the help message for the current mode."""
   
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

def run():
    while True:
        metadata = load_metadata("src/primitive_db/db_meta.json")
        command = prompt.string('Введите команду: ')
        args = shlex.split(command)
        match args[0]:
            case 'exit':
                print('Программа завершена.')
                break

            case 'help':
                print_help()

            case 'create_table':
                if len(args) < 3:
                    print("В таблицу необходимо добавить хотя бы один столбец.")
                    continue
                columns = []
                for col in args[2:]:
                    try:
                        name, type = col.split(":", 1)
                        columns.append((name, type))
                    except ValueError:
                        print(f"Неверный формат столбца: '{col}'. "
                        "Столбец не был обработан.")
                        continue
                create_table(metadata, args[1], columns)
                save_metadata("src/primitive_db/db_meta.json", metadata)

            case 'list_tables':
                if not metadata["tables"]:
                    print("Таблицы отсутствуют.")
                else:
                    print("Список таблиц:")
                    for table in metadata["tables"]:
                        print(f"  - {table}")

            case 'drop_table':
                drop_table(metadata, args[1])
                save_metadata("src/primitive_db/db_meta.json", metadata)

            case _:
                print(f"Неизвестная команда: '{args[0]}'. Введите 'help'.")

        save_metadata("src/primitive_db/db_meta.json", metadata)
        