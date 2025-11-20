import json



def load_table_data(table_name):
    "выгрузка json файла с названием table_name в python словарь"
    filepath = f"src/primitive_db/data/{table_name}.json"
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def save_table_data(table_name, data):
    "загрузка словаря обратно в json файл"
    filepath = f"src/primitive_db/data/{table_name}.json"
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

