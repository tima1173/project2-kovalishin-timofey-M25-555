import shlex

from primitive_db.core import (
    delete,
    insert,
    select,
    update,
)


def parse_value(value: str):
    """
    превращает строчные значения в правильный тип переменных для вставки в таблицу
    для корректной проверки типов
    """
    value = value.strip()
    if value.startswith(("'", '"')) and value.endswith(("'", '"')):
        return value[1:-1]
    if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
        return int(value)
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    return value

def parse_clause(clause: str):
    """
    превращает строку типа key = val в словарь {key:val}
    """
    if '=' not in clause and ':' not in clause:
        return None
    
    if ":" in clause:
        key, val = clause.split(":", 1)
        key = key.strip()
        val = val.strip()
    elif "=" in clause:
        key, val = clause.split("=", 1)
        key = key.strip()
        val = val.strip()

    return {key: val}


def parse_crud(command: str):
    """
    парсинг crud команд
    """
    tokens = shlex.split(command.lower())
    if not tokens:
        return False

    orig_tokens = shlex.split(command)

    try:
        match tokens[0]:
            case "select":
                if len(tokens) < 4 or tokens[1] != "*" or tokens[2] != "from":
                    print("Ошибка: неправильный формат select." \
                    " Используйте: select * from <table> [where ...]")
                    return False

                table_name = orig_tokens[3]

                # Проверяем, есть ли where
                if "where" in tokens:
                    where_idx = tokens.index("where")
                    if where_idx + 1 >= len(orig_tokens):
                        print("Ошибка: указано 'where', но нет условия.")
                        return False
                    where_str = orig_tokens[where_idx + 1]
                    where_clause = parse_clause(where_str)
                    if not where_clause:
                        print(f"Ошибка: не удалось разобрать условие: {where_str}")
                        return False
                else:
                    where_clause = None

                select(table_name, where_clause)

            case "update":
                if len(tokens) < 6 or tokens[2] != "set" or "where" not in tokens:
                    print("Ошибка: неправильный формат update." \
                    " Используйте: update <table> set col=val where col=val")
                    return False

                table_name = orig_tokens[1]
                set_idx = tokens.index("set")
                where_idx = tokens.index("where")

                if set_idx + 1 >= where_idx:
                    print("Ошибка: отсутствует условие SET.")
                    return False

                set_str = orig_tokens[set_idx + 1]
                where_str = orig_tokens[where_idx + 1]

                set_clause = parse_clause(set_str)
                where_clause = parse_clause(where_str)

                if not set_clause:
                    print(f"Ошибка: не удалось разобрать SET условие: {set_str}")
                    return False
                if not where_clause:
                    print(f"Ошибка: не удалось разобрать WHERE условие: {where_str}")
                    return False

                update(table_name, set_clause, where_clause)

            case "delete":
                if len(tokens) < 4 or tokens[1] != "from" or "where" not in tokens:
                    print("Ошибка: неправильный формат delete." \
                    " Используйте: delete from <table> where col=val")
                    return False

                where_idx = tokens.index("where")
                table_name = orig_tokens[2]
                where_str = orig_tokens[where_idx + 1]

                where_clause = parse_clause(where_str)
                if not where_clause:
                    print(f"Ошибка: не удалось разобрать WHERE условие: {where_str}")
                    return False

                delete(table_name, where_clause)

            case "insert":
                if len(tokens) < 3 or tokens[1] != "into" or tokens[3] != "values":
                    print("Ошибка: insert into <table> values <val1> <val2> ...")
                    return False
                table_name = orig_tokens[2]
                values = []
                for val in orig_tokens[4:]:
                    values.append(parse_value(val))
                insert(table_name, values)

            case _:
                print(f"Неизвестная команда: {tokens[0]}. Введите 'help'.")
                return False

    except Exception as e:
        print(f"Ошибка при разборе команды: {e}")
        return False

