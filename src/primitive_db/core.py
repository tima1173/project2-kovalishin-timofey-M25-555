import os
import json
from prettytable import PrettyTable

from primitive_db.utils import load_table_data, save_table_data

def create_table(table_name, columns):
    """Создаёт таблицу в db_meta.json. Добавляет столбец ID по умолчанию."""

    for column in columns:
        if not isinstance(column, (list, tuple)) or len(column) != 2:
                print(f"Ошибка: каждый столбец должен быть парой (имя, тип): {column}")
                return False
        
        col_name, col_type = column
        if col_type not in ("int", "str", "bool"):
            print(f"Ошибка: тип '{col_type}' не поддерживается.")
            return False
        
    full_columns = [("ID", "int")] + columns


    table_data = {
    "columns": full_columns,
    "rows": []
    }

    path = (f"src/primitive_db/data/{table_name}.json")
    if os.path.exists(path):
        print(f"Ошибка: таблица '{table_name}' уже существует.")
        return False
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(table_data, f, indent=4, ensure_ascii=False)

    print(f"Таблица '{table_name}' создана.")
    return True


def drop_table(table_name):
    """
    Удаляет файл таблицы из data/.
    """
    path = f"src/primitive_db/data/{table_name}.json"
    if os.path.exists(path):
        os.remove(path)
        print(f"Файл таблицы '{path}' успешно удалён.")
        return True
    else:
        print(f"Файл таблицы '{path}' не найден. Возможно, он уже был удалён.")
        return False


def list_tables():
    data_dir = "src/primitive_db/data"
    if not os.path.exists(data_dir):
        return []
    # Фильтруем только .json файлы
    tables = [f[:-5] for f in os.listdir(data_dir) if f.endswith(".json")]
    return tables


def insert(table_name, values):
    path = (f"src/primitive_db/data/{table_name}.json")

    #проверяем что таблица существует
    if not os.path.exists(path):
        print(f"Таблицы {table_name} не существует.")
        return False
    
    #проверяем что количество введенных данных совпадает с количеством столбцов в таблице
    metadata = load_table_data(table_name)
    columns = len((metadata)["columns"])
    if len(values) != (columns - 1):
        print(f"Количество введенных данных должно совпадать с количеством столбцов в таблице")
        return False
    
    #проверяем что типы данных соответствуют типам столбцов
    for i, value in enumerate(values):
        if metadata["columns"][i + 1][1] == "int":
            if not isinstance(value, int):
                print(f"Ошибка: значение '{value}' должно быть типом 'int'.")
                return False
        elif metadata["columns"][i + 1][1] == "str":
            if not isinstance(value, str):
                print(f"Ошибка: значение '{value}' должно быть типом 'str'.")
                return False
        elif metadata["columns"][i + 1][1] == "bool":
            if not isinstance(value, bool):
                print(f"Ошибка: значение '{value}' должно быть типом 'bool'.")
                return False

    #добавляем ID к значениям
    if metadata["rows"]:
        last_id = metadata["rows"][-1][0]
        values = [last_id + 1] + values
    else:
        values = [1] + values

    #добавляем данные в таблицу    
    metadata["rows"].append(values)
    save_table_data(table_name, metadata)

def select(table_name, where_clause=None):
    """where_clause = {'variable': 'value'} - выводит только
    записи где значения variable равны value"""

    path = (f"src/primitive_db/data/{table_name}.json")

    #проверяем что таблица существует
    if not os.path.exists(path):
        print(f"Таблицы {table_name} не существует.")
        return False
    
    #выводим данные при пустом where_clause
    if not where_clause:
        metadata = load_table_data(table_name)
        table = PrettyTable()
        table.field_names = [col[0] for col in metadata["columns"]]
        table.add_rows(metadata["rows"])
        print(table)
        return True

    #проверяем корректность where_clause
    if not isinstance(where_clause, dict):
        print(f"Ошибка: where_clause должен быть словарём.")
        return False
    if len(where_clause) != 1:
        print(f"Ошибка: where_clause должен содержать ровно один ключ.")
        return False
    
    #выводим данные при where_clause
    metadata = load_table_data(table_name)
    table = PrettyTable()
    table.field_names = [col[0] for col in metadata["columns"]]

    for i, column in enumerate(metadata["columns"]):
        if column[0] == list(where_clause.keys())[0]:
            column_index = i
            break

    for row in metadata["rows"]:
        if row[column_index] == list(where_clause.values())[0]:
            table.add_row(row)
    print(table)
    return True





def update(table_data, set_clause, where_clause):
    pass

def delete(table_data, where_clause):
    pass


