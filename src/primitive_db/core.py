import os
import json



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