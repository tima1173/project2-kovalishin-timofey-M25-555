
def create_table(metadata, table_name, columns):
    """Создаёт таблицу в db_meta.json. Добавляет столбец ID по умолчанию."""
    if "tables" not in metadata:
        metadata["tables"] = {}

    if table_name in metadata["tables"]:
            print(f"Ошибка: таблица c названием '{table_name}' уже существует.")
            return    
    
    for column in columns:
        if not isinstance(column, (list, tuple)) or len(column) != 2:
                print(f"Ошибка: каждый столбец должен быть парой (имя, тип): {column}")
                return
        for col_name, col_type in columns:
            if col_type not in ("int", "str", "bool"):
                print(f"Ошибка: тип '{col_type}' "
                "не поддерживается. Используйте int, str, или bool")
                return
        
    full_columns = [("ID", "int")] + columns
    metadata["tables"][table_name] = {
    "columns": full_columns,
    "rows": []
    }

    print(f"Таблица '{table_name}' создана.")
    return metadata

def drop_table(metadata, table_name):
    """Удаляет таблицу из db_meta.json."""
    if table_name in metadata["tables"]:
        del metadata["tables"][table_name]
        print(f"Таблица '{table_name}' удалена.")
    else:
        print(f"Ошибка: таблица '{table_name}' не найдена.")
    return metadata
