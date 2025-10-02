"""
1.  **НАЗНАЧЕНИЕ СКРИПТА:**
    Основная цель этого скрипта - автоматически получать (скачивать) подробные данные о российских компаниях по их основному государственному регистрационному номеру (ОГРН) с использованием внешнего сервиса Checko через его программный интерфейс (API). Он разработан так, чтобы эффективно использовать несколько ключей доступа к API, следить за дневными ограничениями на количество запросов для каждого ключа и возобновлять работу с прерванного места, используя список ОГРН из текстового файла.

2.  **ЧТО ДЕЛАЕТ СКРИПТ:**
    Скрипт выполняет следующие шаги:
    - В начале настраивает систему записи событий (логирования) в два разных файла.
    - Загружает список ОГРН компаний, которые нужно обработать, из текстового файла `ogrns.txt`.
    - Загружает из файла `APIs.txt` список доступных ключей доступа к API Checko.
    - Загружает из файла `api_count.json` статистику использования этих ключей за сегодняшний день.
    - Очищает статистику от ключей, которых больше нет в списке активных.
    - Выбирает из доступных ключей тот, у которого наименьшее количество сделанных за сегодня запросов.
    - По очереди обрабатывает каждый выбранный ОГРН:
      - Делает запрос к API Checko для получения данных по этому ОГРН.
      - Обновляет статистику использования ключа на основе ответа API.
      - Если API возвращает ошибку, связанную с превышением лимита, или если текущий ключ достиг лимита, скрипт переключается на следующий доступный ключ.
      - Если API возвращает ошибку, указывающую на недействительность ключа (например, ошибка 401), этот ключ удаляется из системы.
      - Если данные успешно получены, скрипт сохраняет их в сжатом формате JSON.GZ в отдельный файл в папке `JSONs`, имя файла соответствует ОГРН.
      - Делает небольшую паузу перед следующим запросом, чтобы не превышать скорость запросов к API.
    - Если все ОГРН из списка обработаны или все ключи доступа исчерпали дневной лимит, скрипт завершает работу.
    - В конце выводит краткую статистику о проделанной работе в консоль и логи.

3.  **ОСНОВНЫЕ ФУНКЦИИ:**
    Основные функции, определенные в скрипте:
    - `main()`: Главная функция, которая запускает весь процесс, управляет циклом обработки ОГРН, переключением ключей и сохранением прогресса.
    - `load_api_keys(file_path)`: Загружает ключи доступа к API из текстового файла.
    - `load_api_usage(file_path)`: Загружает данные об использовании ключей API из JSON файла.
    - `save_api_usage(api_usage, file_path)`: Сохраняет текущую статистику использования ключей API в JSON файл.
    - `clean_api_usage(api_keys, api_usage)`: Удаляет из статистики ключи, которых больше нет в актуальном списке.
    - `get_available_api_key(api_keys, api_usage)`: Выбирает ключ API, который можно использовать, учитывая дневные лимиты.
    - `update_api_usage(api_key, api_usage, api_usage_file, success=True)`: Обновляет статистику использования конкретного ключа после запроса.
    - `get_company_data(inn, api_key, api_usage, api_usage_file, proxy=None)`: Делает запрос к API Checko для получения данных по конкретному ОГРН.
    - `save_to_json(data, inn)`: Сохраняет полученные данные в JSON.GZ файл.
    - `load_ogrns_from_file(file_path)`: Загружает список ОГРН для обработки из текстового файла.
    - `remove_invalid_api_key(api_key, api_keys, api_usage, api_keys_file)`: Удаляет ключ API из списков и файла, если он оказался недействительным.
    - (Вспомогательные функции для логирования и работы с файлами также используются, но они стандартные или простые)

    Импортированные из модулей:
    - `requests`: Используется для отправки HTTP-запросов к внешнему API Checko.
    - `logging`: Используется для записи подробного журнала работы скрипта.
    - `json`: Используется для работы с данными в формате JSON (чтение/запись статистики ключей и сохранение результатов запросов).
    - `os`: Используется для работы с файловой системой (создание папок, проверка существования файлов, работа с путями).
    - `datetime`, `time`: Используются для работы с датой и временем (формирование имен лог-файлов, отслеживание времени сброса лимитов API).
    - `random`: Используется для добавления случайности (в текущей версии не используется активно, но может применяться для задержек или выбора ключей/прокси).
    - `gzip`: Используется для сжатия JSON-файлов.

4.  **ВХОДНЫЕ ДАННЫЕ:**
    - `ogrns.txt`: Текстовый файл, содержащий список ОГРН компаний, которые нужно обработать. Каждый ОГРН должен быть на отдельной строке.
    - `APIs.txt`: Текстовый файл, содержащий список ключей доступа к API Checko. Каждый ключ должен быть на отдельной строке.
    - `api_count.json`: JSON файл, используемый для хранения статистики использования каждого ключа API (сколько запросов сделано сегодня, когда ожидается сброс лимита). Этот файл создается или обновляется автоматически.

5.  **ВЫХОДНЫЕ ДАННЫЕ:**
    - `JSONs/{ОГРН}.json.gz`: Папка и сжатые JSON.GZ файлы, содержащие полные данные, полученные от API Checko для каждого успешно обработанного ОГРН. Имя каждого файла совпадает с ОГРН компании.
    - `api_count.json`: Обновленный JSON файл со статистикой использования ключей API после каждого запроса.
    - `logs/YYYYMMDD_HHMMSS.log`: Основной файл лога, содержащий все информационные сообщения, предупреждения и ошибки.
    - `logs/issues_report_YYYYMMDD_HHMMSS.log`: Дополнительный файл лога, содержащий только предупреждения (WARNING) и ошибки (ERROR, CRITICAL).

6.  **РАБОЧИЙ ЦИКЛ:**
    Скрипт запускается как обычный Python скрипт из командной строки (или среды разработки), выполняя код в блоке `if __name__ == "__main__":`. У него нет параметров командной строки. Он последовательно проходит по списку ОГРН, полученных из файла, делает запросы, сохраняет данные.

7.  **ВЗАИМОДЕЙСТВИЕ С ДРУГИМИ МОДУЛЯМИ:**
    Скрипт является самостоятельным исполняемым модулем и не предоставляет функции или классы для импорта другими частями проекта. Он активно использует стандартные библиотеки Python (`os`, `json`, `logging` и другие) и внешнюю библиотеку `requests` для взаимодействия с веб-сервисом.

8.  **ЗАВИСИМОСТИ:**
    - Внешние библиотеки: `requests` (необходимо установить с помощью pip: `pip install requests`).
    - Стандартные библиотеки Python: `json`, `os`, `time`, `datetime`, `random`, `logging`, `gzip`.
    - Внутренние модули проекта: Нет (скрипт работает автономно, взаимодействуя с файлами проекта напрямую).

9.  **МЕСТО В АРХИТЕКТУРЕ ПРОЕКТА:**
    Этот скрипт является частью этапа "Извлечение" (Extract) в общем процессе сбора и обработки данных о компаниях. Он отвечает за получение (скачивание) первичных структурированных данных с сервиса Checko. Скрипты, следующие за этим, вероятно, будут заниматься обработкой и анализом скачанных JSON файлов.

10. **ОГРАНИЧЕНИЯ И ОСОБЕННОСТИ:**
    - **Зависимость от API Checko:** Работа скрипта полностью зависит от доступности и стабильности API Checko.
    - **Лимиты API:** Скрипт учитывает дневной лимит запросов на каждый ключ (установлен в коде как 99 запросов). При достижении лимита он пытается переключиться на другой ключ. Если все ключи исчерпаны, работа останавливается.
    - **Обработка ошибок:** Скрипт обрабатывает стандартные ошибки HTTP (например, 401 Unauthorized для недействительных ключей, ошибки превышения лимита). Ошибки запросов (проблемы с сетью, таймауты) также перехватываются.
    - **Отслеживание прогресса:** Прогресс отслеживается через файлы статистики и наличие сохраненных JSON.GZ-файлов. Если скрипт прервется, при следующем запуске он продолжит с необработанных ОГРН (для которых еще нет сохраненных файлов).
    - **Скорость:** Между запросами к API есть фиксированная задержка в 1 секунду для снижения нагрузки на сервер API.
    - **Прокси:** В коде есть упоминание прокси, но в текущей логике они не используются активно для запросов к API Checko (параметр `proxy=None` в `get_company_data`).
"""

import requests
import json
import os
import time
import datetime
import random
import logging
import gzip

import datetime
from datetime import timezone

# --- Настройка логирования ---
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)

current_time_str = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
main_log_file = os.path.join(logs_dir, f"{current_time_str}.log")
issues_log_file = os.path.join(logs_dir, f"issues_report_{current_time_str}.log")

logger = logging.getLogger("api_scraper")
logger.setLevel(logging.DEBUG)

if logger.hasHandlers():
    logger.handlers.clear()

main_file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
main_file_handler = logging.FileHandler(main_log_file, encoding='utf-8')
main_file_handler.setLevel(logging.DEBUG)
main_file_handler.setFormatter(main_file_formatter)
logger.addHandler(main_file_handler)

issues_file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
issues_file_handler = logging.FileHandler(issues_log_file, encoding='utf-8')
issues_file_handler.setLevel(logging.WARNING)
issues_file_handler.setFormatter(issues_file_formatter)
logger.addHandler(issues_file_handler)

console_formatter = logging.Formatter('%(levelname)s: %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

logger.info(f"Начало работы скрипта. Основной лог: {main_log_file}, Отчет об ошибках: {issues_log_file}")
# --- Конец настройки логирования ---

API_URL = "https://api.checko.ru/v2/company"
API_DAILY_LIMIT = 99
API_RESET_HOURS = 25  # 24 часа + 1 час запаса для гарантированного сброса

def load_api_keys(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            api_keys = [line.strip() for line in f if line.strip()]
        logger.info(f"Загружено {len(api_keys)} API-ключей из файла {file_path}")
        return api_keys
    except Exception as e:
        logger.error(f"Ошибка при загрузке списка API-ключей из файла {file_path}: {e}")
        return []

def load_api_usage(file_path):
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        logger.info(f"Файл статистики {file_path} не существует. Создаем новый.")
        return {}
    except Exception as e:
        logger.error(f"Ошибка при загрузке статистики использования API-ключей: {e}")
        return {}

def save_api_usage(api_usage, file_path):
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(api_usage, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка при сохранении статистики использования API-ключей: {e}")

def clean_api_usage(api_keys, api_usage):
    keys_to_remove = [key for key in api_usage if key not in api_keys]
    if keys_to_remove:
        for key in keys_to_remove:
            del api_usage[key]
        logger.info(f"Удалено {len(keys_to_remove)} неактуальных API-ключей из статистики: {', '.join(keys_to_remove)}")
    return api_usage

def get_available_api_key(api_keys, api_usage):
    now = datetime.datetime.now(timezone.utc)
    available_keys = []
    
    for key in api_keys:
        if key not in api_usage:
            api_usage[key] = {"total_requests": 0, "today_requests": 0, "last_used": None, "last_reset": None}
        
        key_data = api_usage[key]
        
        if "next_reset" in key_data and key_data["next_reset"]:
            try:
                next_reset_time = datetime.datetime.fromisoformat(key_data["next_reset"])
                if next_reset_time.tzinfo is None: # Если время из файла naive (без часового пояса)
                    next_reset_time = next_reset_time.replace(tzinfo=timezone.utc) # Делаем его aware UTC
                else: # Если время уже aware, убедимся, что оно в UTC
                    next_reset_time = next_reset_time.astimezone(timezone.utc)

                if now >= next_reset_time:
                    logger.info(f"Сброс счетчика для ключа ...{key[-4:]} по таймеру (UTC).")
                    key_data["today_requests"] = 0
                    key_data.pop("next_reset", None)
                    key_data["last_reset"] = now.isoformat()
            except (ValueError, TypeError):
                 logger.warning(f"Некорректный формат времени сброса для ключа ...{key[-4:]}. Сбрасываем счетчик.")
                 key_data["today_requests"] = 0
                 key_data.pop("next_reset", None)

        if key_data["today_requests"] < API_DAILY_LIMIT:
            available_keys.append((key, key_data["today_requests"]))
    
    if available_keys:
        available_keys.sort(key=lambda x: x[1])
        selected_key = available_keys[0][0]
        logger.debug(f"Выбран API-ключ: ...{selected_key[-4:]}, использовано запросов: {api_usage[selected_key]['today_requests']}")
        return selected_key
    
    logger.warning("Все API-ключи достигли дневного лимита запросов!")
    return None

# ИЗМЕНЕНО: Исправлена логика расчета времени сброса лимита на "плавающее окно 24+1 час"
def update_api_usage(api_key, api_usage, api_usage_file, success=True):
    """Обновляет статистику использования API-ключа."""
    now = datetime.datetime.now(timezone.utc)
    if api_key not in api_usage:
        api_usage[api_key] = {"total_requests": 0, "today_requests": 0, "last_used": None, "last_reset": None}
    
    if success:
        api_usage[api_key]["total_requests"] += 1
        api_usage[api_key]["today_requests"] += 1
        api_usage[api_key]["last_used"] = now.isoformat()
        
        if api_usage[api_key]["today_requests"] >= API_DAILY_LIMIT:
            # Устанавливаем сброс ровно через API_RESET_HOURS (25) часов от текущего момента
            next_reset_time = now + datetime.timedelta(hours=API_RESET_HOURS)
            api_usage[api_key]["next_reset"] = next_reset_time.isoformat()
            logger.warning(f"Достигнут дневной лимит для ключа ...{api_key[-4:]}. Следующий сброс в UTC: {next_reset_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    save_api_usage(api_usage, api_usage_file)
    return api_usage

def get_company_data(ogrn, api_key, api_usage, api_usage_file, proxy=None):
    params = {"key": api_key, "ogrn": ogrn, "source": "true"}
    proxies = {'http': f'http://{proxy}', 'https': f'http://{proxy}'} if proxy else None
    
    logger.debug(f"Запрос ОГРН {ogrn} [Ключ: ...{api_key[-4:]}]...")
    try:
        response = requests.get(API_URL, params=params, proxies=proxies, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("meta", {}).get("status") == "ok":
            today_request_count = data.get("meta", {}).get("today_request_count", 0)
            
            if api_key in api_usage and api_usage[api_key]["today_requests"] > today_request_count:
                logger.info(f"Обнаружен сброс счетчика API для ключа ...{api_key[-4:]}: {api_usage[api_key]['today_requests']} -> {today_request_count}")
                api_usage[api_key]["last_reset"] = datetime.datetime.now(timezone.utc).isoformat()
                api_usage[api_key]["today_requests"] = 0 # Сбрасываем счетчик, т.к. API его сбросил
            
            if api_key in api_usage:
                api_usage[api_key]["today_requests"] = today_request_count
            
            api_usage = update_api_usage(api_key, api_usage, api_usage_file, True)
            needs_switch = today_request_count >= API_DAILY_LIMIT
            if needs_switch:
                logger.warning(f"API-ключ ...{api_key[-4:]} достиг лимита. Необходимо переключение.")
            
            return data, api_usage, needs_switch, False
        else:
            error_message = data.get("meta", {}).get("message", "Неизвестная ошибка")
            logger.warning(f"Ошибка API для ОГРН {ogrn}: {error_message}")
            needs_switch = "limit exceeded" in error_message.lower() or "daily limit" in error_message.lower()
            if needs_switch:
                logger.warning(f"Обнаружено превышение лимита для ключа ...{api_key[-4:]}.")
                if api_key in api_usage:
                    api_usage[api_key]["today_requests"] = API_DAILY_LIMIT
                    # Запускаем обновление, чтобы установилось время сброса
                    api_usage = update_api_usage(api_key, api_usage, api_usage_file, False)


            return None, api_usage, needs_switch, False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса для ОГРН {ogrn}: {e}")
        key_invalid = hasattr(e, 'response') and e.response is not None and e.response.status_code == 401
        if key_invalid:
            logger.error(f"Ошибка 401 Unauthorized для ключа ...{api_key[-4:]}. Ключ недействителен.")
            if api_key in api_usage:
                api_usage[api_key]["today_requests"] = API_DAILY_LIMIT
            return None, api_usage, True, True
        return None, api_usage, False, False
    
    except json.JSONDecodeError:
        logger.error(f"Ошибка при разборе JSON-ответа для ОГРН {ogrn}")
        return None, api_usage, False, False

def save_to_json(data, ogrn):
    filename = f"{ogrn}.json.gz"
    full_path = os.path.join("JSONs", filename)
    
    try:
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with gzip.open(full_path, 'wt', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Успешно: данные для ОГРН {ogrn} сохранены в {filename}.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в файл {full_path}: {e}")

def remove_invalid_api_key(api_key, api_keys, api_usage, api_keys_file):
    logger.warning(f"УДАЛЕНИЕ недействительного API-ключа: ...{api_key[-4:]}")
    if api_key in api_usage: del api_usage[api_key]
    if api_key in api_keys: api_keys.remove(api_key)
    try:
        if os.path.exists(api_keys_file):
            with open(api_keys_file, 'r', encoding='utf-8') as f: lines = f.readlines()
            new_lines = [line for line in lines if line.strip() != api_key]
            if len(lines) != len(new_lines):
                with open(api_keys_file, 'w', encoding='utf-8') as f: f.writelines(new_lines)
                logger.info(f"API-ключ ...{api_key[-4:]} удален из файла {api_keys_file}")
    except Exception as e:
        logger.error(f"Ошибка при удалении API-ключа ...{api_key[-4:]} из файла {api_keys_file}: {e}")
    return api_keys, api_usage

def load_ogrns_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            ogrns_list = [line.strip() for line in f if line.strip()]
        logger.info(f"Загружено {len(ogrns_list)} ОГРН из файла {file_path} для обработки.")
        return ogrns_list
    except Exception as e:
        logger.error(f"Ошибка при загрузке списка ОГРН из файла {file_path}: {e}")
        return []

def main():
    api_keys_file = "APIs.txt"
    api_usage_file = "api_count.json"
    ogrns_file_path = "ogrns.txt"
    output_dir = "JSONs"

    os.makedirs(output_dir, exist_ok=True)

    ogrns_list = load_ogrns_from_file(ogrns_file_path)
    if not ogrns_list:
        logger.critical("Список ОГРН для обработки пуст. Завершение работы.")
        return

    total_ogrns = len(ogrns_list)

    api_keys = load_api_keys(api_keys_file)
    if not api_keys:
        logger.critical("Не удалось загрузить API-ключи. Проверьте файл APIs.txt.")
        return

    api_usage = load_api_usage(api_usage_file)
    api_usage = clean_api_usage(api_keys, api_usage)
    save_api_usage(api_usage, api_usage_file)

    current_api_key = get_available_api_key(api_keys, api_usage)
    if not current_api_key:
        logger.critical("Нет доступных API-ключей. Все ключи превысили дневной лимит.")
        return

    successful = 0
    failed = 0
    api_switches = 0
    removed_keys = 0
    
    for i, ogrn in enumerate(ogrns_list):
        logger.info(f"Обработка {i + 1}/{total_ogrns}. ОГРН: {ogrn}")

        output_filename = f"{ogrn}.json.gz"
        full_output_path = os.path.join(output_dir, output_filename)

        if os.path.exists(full_output_path):
            logger.debug(f"ОГРН {ogrn} уже обработан, файл существует. Пропускаем.")
            successful += 1
            continue

        max_attempts = len(api_keys) if api_keys else 1
        for attempt in range(max_attempts):
            if not current_api_key:
                logger.critical("Все API-ключи исчерпаны. Завершаем обработку.")
                break
            
            company_data, api_usage, needs_switch, key_invalid = get_company_data(
                ogrn, current_api_key, api_usage, api_usage_file, None
            )
            
            if key_invalid:
                api_keys, api_usage = remove_invalid_api_key(current_api_key, api_keys, api_usage, api_keys_file)
                removed_keys += 1
                current_api_key = get_available_api_key(api_keys, api_usage)
                api_switches += 1
                if current_api_key:
                    continue
                else:
                    break
            
            save_api_usage(api_usage, api_usage_file)
            
            if company_data:
                save_to_json(company_data, ogrn)
                successful += 1
                if needs_switch:
                    current_api_key = get_available_api_key(api_keys, api_usage)
                    api_switches += 1
                break
            
            if needs_switch:
                current_api_key = get_available_api_key(api_keys, api_usage)
                api_switches += 1
                if current_api_key:
                    continue

            if attempt == max_attempts - 1 or not needs_switch:
                logger.error(f"Не удалось получить данные для ОГРН {ogrn} после {attempt + 1} попыток.")
                failed += 1
                break
        
        if not current_api_key:
            logger.critical("Все API-ключи исчерпаны. Завершаем обработку.")
            break
        
        time.sleep(1)
    
    logger.info("\n------ Обработка завершена ------")
    logger.info(f"Всего ОГРН в списке: {total_ogrns}")
    logger.info(f"Успешно обработано (включая ранее скачанные): {successful}")
    logger.info(f"Не удалось обработать: {failed}")
    logger.info(f"Количество переключений API-ключей: {api_switches}")
    logger.info(f"Количество удаленных недействительных API-ключей: {removed_keys}")
    logger.info("-----------------------------------")

if __name__ == "__main__":
    main()