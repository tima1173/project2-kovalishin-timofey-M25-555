# src/primitive_db/engine.py
import shlex
import prompt
import os

from primitive_db.core import create_table, drop_table, list_tables


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
    data_dir = "src/primitive_db/data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    while True:
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
                        columns.append((name.strip(), type.strip()))
                    except ValueError:
                        print(f"Неверный формат столбца: '{col}'. "
                        "Столбец не был обработан.")
                        break
                else:
                    create_table(args[1], columns)

            case 'list_tables':
                tables = list_tables()
                if tables:
                    print("Список таблиц:")
                    for table in tables:
                        print(f"  - {table}")
                else:
                    print("Таблицы отсутствуют.")

            case 'drop_table':
                if len(args) != 2:
                    print("Неверное количество аргументов. "
                    "Использование: drop_table <имя_таблицы>")
                else:
                    drop_table(args[1])

            case _:
                print(f"Неизвестная команда: '{args[0]}'. Введите 'help'.")


        