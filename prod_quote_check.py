import requests
import pandas as pd
import json
import random
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

# Настройки
environment = "prod"  # staging/prod/dev
last_amount = 1000  # Сумма обмена в USD
enable_exceptions = False  # Включить/выключить исключения
enable_filters = False  # Включить/выключить фильтры
enable_exceptions1 = True  # сверяем с дополнительной апишкой

stop_threads = False


def signal_handler(sig, frame):
    global stop_threads
    stop_threads = True
    print("Получен сигнал остановки. Завершение потоков...")


signal.signal(signal.SIGINT, signal_handler)  # Обработка сигнала прерывания (Ctrl+C)


# Загрузка исключений из файла
if enable_exceptions:
    exceptions_df = pd.read_excel(
        "C:\\Users\\unton\\Downloads\\Trading_bot\\exceptions1.xlsx"
    )
    exceptions = set(
        zip(
            exceptions_df["from_symbol"],
            exceptions_df["to_symbol"],
            exceptions_df["from_chain_id"],
            exceptions_df["to_chain_id"],
        )
    )

if enable_exceptions1:
    url_cryptos1 = f"https://resolver.{environment}.kxlabs.org/api/v0/quote/pairs"
    response1 = requests.get(url_cryptos1)
    response1.raise_for_status()  # Проверка успешности запроса
    data1 = response1.json().get("result", {})

    # Приводим ключи в data1 к нижнему регистру для сравнения
    data_lower = {k.lower(): [v_item.lower() for v_item in v] for k, v in data1.items()}

# Определение URL для первого запроса
url_cryptos = f"https://crypto.{environment}.swaps.io/api/v0/cryptos"

# Список для хранения отчетных данных
report_data = []

# Переменные для отслеживания комбинаций
total_combinations = 0  # Общее количество комбинаций (фиксированное)
checked_combinations = 0  # Количество проверенных комбинаций

# Отправка первого GET-запроса
response_cryptos = requests.get(url_cryptos)

# Проверка успешности запроса
if response_cryptos.status_code == 200:
    data = response_cryptos.json()
    cryptos = data.get("cryptos", [])

    if isinstance(cryptos, list) and len(cryptos) >= 4:
        # Подсчет общего количества комбинаций
        total_combinations = len(cryptos) * (
            len(cryptos) - 1
        )  # Все возможные комбинации без повторений

        def process_combination(i, j):
            global stop_threads
            if stop_threads:
                return None

            from_block = cryptos[i]
            from_address = from_block.get("address")
            from_chain_id = from_block.get("chain_id")
            from_symbol = from_block.get("symbol")
            from_decimals = from_block.get("decimals")

            to_block = cryptos[j]
            to_address = to_block.get("address")
            to_chain_id = to_block.get("chain_id")
            to_symbol = to_block.get("symbol")
            to_decimals = to_block.get("decimals")

            # Проверка на допустимые значения (фильтры)
            if enable_filters and (
                from_chain_id
                not in [
                    "10",
                ]
                or to_chain_id
                not in [
                    "10",
                ]
            ):
                return None

            # "1","10","100","137","56","8453","81457","43114","200901","60808","42161","668467","223",
            # Проверка на наличие связки в исключениях
            if (
                enable_exceptions
                and (
                    from_symbol,
                    to_symbol,
                    int(from_chain_id),
                    int(to_chain_id),
                )
                not in exceptions
            ):
                print(
                    f"Пропускаем связку: {from_chain_id}, {from_symbol} -> {to_chain_id}, {to_symbol}"
                )
                return None

            if enable_exceptions1:
                from_key = f"{from_chain_id}/{from_address}".lower()
                to_key = f"{to_chain_id}/{to_address}".lower()
                # Проверяем наличие from_key в data_lower
                if from_key in data_lower:
                    # Проверяем наличие to_key в значениях data_lower[from_key]
                    if to_key in data_lower[from_key]:
                        print(f"Найдена пара: {from_key} -> {to_key}")
                        # Здесь можно продолжить обработку
                    else:
                        print(f"{to_key} не найдено в {from_key}. Пропускаем...")
                        return None
                else:
                    print(f"{from_key} не найдено в результате. Пропускаем...")
                    return None

            if from_address and from_chain_id and to_address and to_chain_id:
                global checked_combinations
                checked_combinations += (
                    1  # Увеличиваем количество проверенных комбинаций
                )

                # Вывод текущего состояния
                print(
                    f"Проверяем комбинацию {checked_combinations}/{total_combinations}: {from_symbol} -> {to_symbol}"
                )
                if stop_threads:
                    return None
                # Запрос на получение цен
                url_price_map = f"https://meta2.{environment}.swaps.io/api/price/map"
                response_price_map = requests.get(url_price_map, timeout=1800)

                if response_price_map.status_code == 200:
                    prices = response_price_map.json().get("prices", {})
                    price_id = f"{from_chain_id}.{from_address}"
                    to_price_id = f"{to_chain_id}.{to_address}"

                    if price_id in prices and to_price_id in prices:
                        value = prices[price_id].get("value")
                        to_value = prices[to_price_id].get("value")

                        if value is not None:
                            value = float(value)
                            to_value = float(to_value)
                            adjusted_value = (
                                last_amount / (value / 10**8) * 10**from_decimals
                            )
                            adjusted_value = f"{adjusted_value:.0f}"

                            # Формирование URL для второго запроса
                            url_quote = f"https://resolver.{environment}.kxlabs.org/api/v0/quote"
                            params = {
                                "fromChainId": from_chain_id,
                                "fromTokenAddress": from_address,
                                "fromAmount": str(adjusted_value),
                                "toChainId": to_chain_id,
                                "toTokenAddress": to_address,
                                "fromActor": "0x2972640996e5Db677b50dA577635b90b1319892A",
                                "fromActorReceiver": "0x2972640996e5Db677b50dA577635b90b1319892A",
                                "_debug": "true",
                            }

                            # Переменные для повторных попыток
                            max_attempts = 2
                            attempt = 0
                            success = False

                            while attempt < max_attempts and not success:

                                # Генерация случайного trace id
                                trace_id = f"tst{random.randint(1000, 9999)}{random.randint(1000, 9999)}"

                                headers = {
                                    "X-Request-ID": trace_id  # Добавляем trace id в заголовок
                                }

                                response_quote = requests.get(
                                    url_quote,
                                    params=params,
                                    headers=headers,
                                    timeout=1800,
                                )
                                response_quote1 = requests.get(
                                    url_quote, params=params, timeout=1800
                                )

                                if response_quote.status_code == 200:
                                    quote_data = response_quote.json()
                                    print(
                                        from_chain_id,
                                        from_symbol,
                                        "->",
                                        to_chain_id,
                                        to_symbol,
                                    )

                                    # Проверяем, что _debug существует и не является None
                                    debug_info = quote_data.get("_debug")
                                    if debug_info is not None:
                                        toAmountDecimal = debug_info.get(
                                            "toAmountDecimal", {}
                                        )

                                        # Проверяем, если toAmountDecimal не None и содержит "data"
                                        if toAmountDecimal is not None:
                                            to_to_amount = toAmountDecimal

                                            if to_to_amount is not None:
                                                to_to_amount = float(to_to_amount)
                                                to_last_amount = to_to_amount * (
                                                    to_value / 10**8
                                                )

                                                lower_bound = last_amount * 0.85
                                                upper_bound = last_amount * 1.15

                                                # Считаем просадку
                                                drawdown = 100 - (
                                                    to_last_amount / last_amount * 100
                                                )

                                                total_loss_usd_SWAPS = (
                                                    last_amount - to_last_amount
                                                )

                                                fee_details = debug_info.get(
                                                    "feeDetails", {}
                                                )
                                                src_chain_swap_fee = fee_details.get(
                                                    "srcChainSwapFee", None
                                                )
                                                dst_chain_swap_fee = fee_details.get(
                                                    "dstChainSwapFee", None
                                                )
                                                trading_fee_percentage = (
                                                    fee_details.get(
                                                        "tradingFeePercentage",
                                                        None,
                                                    )
                                                )
                                                from_gas_fee = fee_details.get(
                                                    "fromGasFee", None
                                                )
                                                to_gas_fee = fee_details.get(
                                                    "toGasFee", None
                                                )

                                                src_swap_operation = debug_info.get(
                                                    "srcSwapOperation"
                                                )
                                                src_router = None
                                                if (
                                                    src_swap_operation is not None
                                                    and "data" in src_swap_operation
                                                ):
                                                    src_router = src_swap_operation[
                                                        "data"
                                                    ].get("router", None)

                                                dst_swap_operation = debug_info.get(
                                                    "dstSwapOperation"
                                                )
                                                dst_router = None
                                                if (
                                                    dst_swap_operation is not None
                                                    and "data" in dst_swap_operation
                                                ):
                                                    dst_router = dst_swap_operation[
                                                        "data"
                                                    ].get("router", None)

                                                # Извлечение candidatesOrdered
                                                candidates_ordered_str = debug_info.get(
                                                    "onchainSwapDetails", {}
                                                ).get(
                                                    "candidatesOrdered",
                                                    None,
                                                )

                                                # Преобразование строки в список, если candidatesOrdered существует
                                                candidates_ordered = (
                                                    json.loads(candidates_ordered_str)
                                                    if candidates_ordered_str
                                                    else []
                                                )

                                                # Извлечение первого элемента
                                                first_candidate = (
                                                    candidates_ordered[0]
                                                    if candidates_ordered
                                                    else None
                                                )
                                                swap_type = None
                                                from_liq_token = None
                                                to_liq_token = None

                                                # Извлечение fromCT и toCT из onchainSwapDetails
                                                onchain_swap_details = debug_info.get("onchainSwapDetails", {})
                                                from_ct = onchain_swap_details.get("fromCT", None)
                                                to_ct = onchain_swap_details.get("toCT", None)

                                                if first_candidate:
                                                    # Извлекаем значения из first_candidate
                                                    swap_type = first_candidate[0]  # 5_onchainswap_to_onchainswap
                                                    from_liq_token = first_candidate[1] if first_candidate[1] else from_ct
                                                    to_liq_token = first_candidate[2] if first_candidate[2] else to_ct 

                                                report_data.append(
                                                    {
                                                        "Trace ID": trace_id,
                                                        "URL": response_quote1.url,
                                                        "From Symbol": from_symbol,
                                                        "To Symbol": to_symbol,
                                                        "From Amount (USD)": last_amount,
                                                        "To Amount (USD)": to_last_amount,
                                                        "drawdown (%)": drawdown,
                                                        "SWAPS - Total Loss (USD)": total_loss_usd_SWAPS,
                                                        "From Chain ID": from_chain_id,
                                                        "To Chain ID": to_chain_id,
                                                        "src_chain_swap_fee": src_chain_swap_fee,
                                                        "dst_chain_swap_fee": dst_chain_swap_fee,
                                                        "trading_fee_percentage": trading_fee_percentage,
                                                        "from_gas_fee": from_gas_fee,
                                                        "to_gas_fee": to_gas_fee,
                                                        "swap_type": swap_type,
                                                        "from_liq_token": from_liq_token,
                                                        "to_liq_token": to_liq_token,
                                                        "src_router": src_router,
                                                        "dst_router": dst_router,
                                                        "Status": "Success",
                                                    }
                                                )

                                            else:
                                                report_data.append(
                                                    {
                                                        "Trace ID": trace_id,
                                                        "URL": response_quote1.url,
                                                        "From Symbol": from_symbol,
                                                        "To Symbol": to_symbol,
                                                        "From Amount (USD)": last_amount,
                                                        "To Amount (USD)": None,
                                                        "drawdown (%)": None,
                                                        "SWAPS - Total Loss (USD)": None,
                                                        "From Chain ID": from_chain_id,
                                                        "To Chain ID": to_chain_id,
                                                        "src_chain_swap_fee": None,
                                                        "dst_chain_swap_fee": None,
                                                        "trading_fee_percentage": None,
                                                        "from_gas_fee": None,
                                                        "to_gas_fee": None,
                                                        "swap_type": None,
                                                        "from_liq_token": None,
                                                        "to_liq_token": None,
                                                        "src_router": None,
                                                        "dst_router": None,
                                                        "Status": "to_amount is None",
                                                    }
                                                )
                                        else:
                                            report_data.append(
                                                {
                                                    "Trace ID": trace_id,
                                                    "URL": response_quote1.url,
                                                    "From Symbol": from_symbol,
                                                    "To Symbol": to_symbol,
                                                    "From Amount (USD)": last_amount,
                                                    "To Amount (USD)": to_last_amount,
                                                    "drawdown (%)": drawdown,
                                                    "SWAPS - Total Loss (USD)": total_loss_usd_SWAPS,
                                                    "From Chain ID": from_chain_id,
                                                    "To Chain ID": to_chain_id,
                                                    "src_chain_swap_fee": src_chain_swap_fee,
                                                    "dst_chain_swap_fee": dst_chain_swap_fee,
                                                    "trading_fee_percentage": trading_fee_percentage,
                                                    "from_gas_fee": from_gas_fee,
                                                    "to_gas_fee": to_gas_fee,
                                                    "swap_type": swap_type,
                                                    "from_liq_token": from_liq_token,
                                                    "to_liq_token": to_liq_token,
                                                    "src_router": src_router,
                                                    "dst_router": dst_router,
                                                    "Status": "Success",
                                                }
                                            )
                                    else:
                                        report_data.append(
                                            {
                                                "Trace ID": trace_id,
                                                "URL": response_quote1.url,
                                                "From Symbol": from_symbol,
                                                "To Symbol": to_symbol,
                                                "From Amount (USD)": last_amount,
                                                "To Amount (USD)": to_last_amount,
                                                "drawdown (%)": drawdown,
                                                "SWAPS - Total Loss (USD)": total_loss_usd_SWAPS,
                                                "From Chain ID": from_chain_id,
                                                "To Chain ID": to_chain_id,
                                                "src_chain_swap_fee": src_chain_swap_fee,
                                                "dst_chain_swap_fee": dst_chain_swap_fee,
                                                "trading_fee_percentage": trading_fee_percentage,
                                                "from_gas_fee": from_gas_fee,
                                                "to_gas_fee": to_gas_fee,
                                                "swap_type": swap_type,
                                                "from_liq_token": from_liq_token,
                                                "to_liq_token": to_liq_token,
                                                "src_router": src_router,
                                                "dst_router": dst_router,
                                                "Status": "_debug is None",
                                            }
                                        )
                                    success = True

                                else:
                                    print(
                                        trace_id,
                                        from_chain_id,
                                        from_symbol,
                                        "->",
                                        to_chain_id,
                                        to_symbol,
                                        "attempts:",
                                        response_quote.status_code,
                                        "-",
                                        response_quote.text,
                                    )
                                    attempt += 1
                                    if attempt == max_attempts:
                                        report_data.append(
                                            {
                                                "Trace ID": trace_id,
                                                "URL": response_quote1.url,
                                                "From Symbol": from_symbol,
                                                "To Symbol": to_symbol,
                                                "From Amount (USD)": last_amount,
                                                "To Amount (USD)": None,
                                                "drawdown (%)": None,
                                                "SWAPS - Total Loss (USD)": None,
                                                "From Chain ID": from_chain_id,
                                                "To Chain ID": to_chain_id,
                                                "src_chain_swap_fee": None,
                                                "dst_chain_swap_fee": None,
                                                "trading_fee_percentage": None,
                                                "from_gas_fee": None,
                                                "to_gas_fee": None,
                                                "swap_type": None,
                                                "from_liq_token": None,
                                                "to_liq_token": None,
                                                "src_router": None,
                                                "dst_router": None,
                                                "Status": f"attempts: {response_quote.status_code} - {response_quote.text}",
                                            }
                                        )
                        else:
                            report_data.append(
                                {
                                    "Trace ID": None,
                                    "URL": response_quote1.url,
                                    "From Symbol": from_symbol,
                                    "To Symbol": to_symbol,
                                    "From Amount (USD)": None,
                                    "To Amount (USD)": None,
                                    "drawdown (%)": None,
                                    "SWAPS - Total Loss (USD)": None,
                                    "From Chain ID": from_chain_id,
                                    "To Chain ID": to_chain_id,
                                    "src_chain_swap_fee": None,
                                    "dst_chain_swap_fee": None,
                                    "trading_fee_percentage": None,
                                    "from_gas_fee": None,
                                    "to_gas_fee": None,
                                    "swap_type": None,
                                    "from_liq_token": None,
                                    "to_liq_token": None,
                                    "src_router": None,
                                    "dst_router": None,
                                    "Status": "Value is None (/api/price/map)",
                                }
                            )

                    else:
                        report_data.append(
                            {
                                "Trace ID": None,
                                "URL": response_quote1.url,
                                "From Symbol": from_symbol,
                                "To Symbol": to_symbol,
                                "From Amount (USD)": last_amount,
                                "To Amount (USD)": None,
                                "drawdown (%)": None,
                                "SWAPS - Total Loss (USD)": None,
                                "From Chain ID": from_chain_id,
                                "To Chain ID": to_chain_id,
                                "src_chain_swap_fee": None,
                                "dst_chain_swap_fee": None,
                                "trading_fee_percentage": None,
                                "from_gas_fee": None,
                                "to_gas_fee": None,
                                "swap_type": None,
                                "from_liq_token": None,
                                "to_liq_token": None,
                                "src_router": None,
                                "dst_router": None,
                                "Status": "Price not found",
                            }
                        )
            else:
                report_data.append(
                    {
                        "Trace ID": None,
                        "URL": response_quote1.url,
                        "From Symbol": from_symbol,
                        "To Symbol": to_symbol,
                        "From Amount (USD)": None,
                        "To Amount (USD)": None,
                        "drawdown (%)": None,
                        "SWAPS - Total Loss (USD)": None,
                        "From Chain ID": from_chain_id,
                        "To Chain ID": to_chain_id,
                        "src_chain_swap_fee": None,
                        "dst_chain_swap_fee": None,
                        "trading_fee_percentage": None,
                        "from_gas_fee": None,
                        "to_gas_fee": None,
                        "swap_type": None,
                        "from_liq_token": None,
                        "to_liq_token": None,
                        "src_router": None,
                        "dst_router": None,
                        "Status": "Invalid addresses or chain IDs",
                    }
                )

        # Используем ThreadPoolExecutor для обработки комбинаций
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for i in range(len(cryptos)):
                for j in range(len(cryptos)):
                    if i != j:
                        futures.append(executor.submit(process_combination, i, j))

            try:
                for future in as_completed(futures):
                    if stop_threads:  # Проверяем флаг на остановку
                        print("Обработка прервана.")
                        break
                    result = future.result()
                    if result:
                        print(result)
            except Exception as e:
                print(f"Произошла ошибка: {e}")

    else:
        print("Недостаточно криптовалют для обработки.")
else:
    print(
        f"Ошибка при получении криптовалют: {response_cryptos.status_code} - {response_cryptos.text}"
    )

print(f"Общее количество комбинаций: {total_combinations}")
print(f"Количество проверенных комбинаций: {checked_combinations}")


# Интеграция с Google Sheets
def save_to_google_sheets(report_data):
    # Определяем область доступа и загружаем учетные данные
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    
    credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(credentials_json), scope)
    client = gspread.authorize(creds)
    # Получаем текущую дату в формате "YYYY-MM-DD"
    current_date = datetime.now().strftime("%Y-%m-%d")
    sheet_base_name = f"Report {current_date}"  # Формируем базовое имя листа

    # Создаем новую таблицу или открываем существующую
    spreadsheet = client.open("kinetex quote-check")  # Создает новую таблицу

    # Проверяем, существует ли лист с заданным именем
    sheet_name = sheet_base_name
    existing_sheets = [sheet.title for sheet in spreadsheet.worksheets()]  # Получаем список всех листов

    # Увеличиваем счетчик, если лист с таким именем уже существует
    counter = 1
    while sheet_name in existing_sheets:
        sheet_name = f"{sheet_base_name} ({counter})"
        counter += 1

    # Создаем новый лист с уникальным именем
    sheet = spreadsheet.add_worksheet(title=sheet_name, rows="10000", cols="20")

    # Записываем заголовки
    headers = [
        "Trace ID",
        "URL",
        "From Symbol",
        "To Symbol",
        "From Amount (USD)",
        "To Amount (USD)",
        "drawdown (%)",
        "SWAPS - Total Loss (USD)",
        "From Chain ID",
        "To Chain ID",
        "src_chain_swap_fee",
        "dst_chain_swap_fee",
        "trading_fee_percentage",
        "from_gas_fee",
        "to_gas_fee",
        "swap_type",
        "from_liq_token",
        "to_liq_token",
        "src_router",
        "dst_router",
        "Status",
    ]
    sheet.append_row(headers)

    # Записываем данные
    rows_to_insert = []
    for report in report_data:
        # Преобразуем списки в строки
        row = [
            ', '.join(report["Trace ID"]) if isinstance(report["Trace ID"], list) else report["Trace ID"] or "",
            ', '.join(report["URL"]) if isinstance(report["URL"], list) else report["URL"] or "",
            ', '.join(report["From Symbol"]) if isinstance(report["From Symbol"], list) else report["From Symbol"] or "",
            ', '.join(report["To Symbol"]) if isinstance(report["To Symbol"], list) else report["To Symbol"] or "",
            ', '.join(report["From Amount (USD)"]) if isinstance(report["From Amount (USD)"], list) else report["From Amount (USD)"] or "",
            ', '.join(report["To Amount (USD)"]) if isinstance(report["To Amount (USD)"], list) else report["To Amount (USD)"] or "",
            ', '.join(report["drawdown (%)"]) if isinstance(report["drawdown (%)"], list) else report["drawdown (%)"] or "",
            ', '.join(report["SWAPS - Total Loss (USD)"]) if isinstance(report["SWAPS - Total Loss (USD)"], list) else report["SWAPS - Total Loss (USD)"] or "",
            ', '.join(report["From Chain ID"]) if isinstance(report["From Chain ID"], list) else report["From Chain ID"] or "",
            ', '.join(report["To Chain ID"]) if isinstance(report["To Chain ID"], list) else report["To Chain ID"] or "",
            ', '.join(report["src_chain_swap_fee"]) if isinstance(report["src_chain_swap_fee"], list) else report["src_chain_swap_fee"] or "",
            ', '.join(report["dst_chain_swap_fee"]) if isinstance(report["dst_chain_swap_fee"], list) else report["dst_chain_swap_fee"] or "",
            ', '.join(report["trading_fee_percentage"]) if isinstance(report["trading_fee_percentage"], list) else report["trading_fee_percentage"] or "",
            ', '.join(report["from_gas_fee"]) if isinstance(report["from_gas_fee"], list) else report["from_gas_fee"] or "",
            ', '.join(report["to_gas_fee"]) if isinstance(report["to_gas_fee"], list) else report["to_gas_fee"] or "",
            ', '.join(report["swap_type"]) if isinstance(report["swap_type"], list) else report["swap_type"] or "",
            ', '.join(report["from_liq_token"]) if isinstance(report["from_liq_token"], list) else report["from_liq_token"] or "",
            ', '.join(report["to_liq_token"]) if isinstance(report["to_liq_token"], list) else report["to_liq_token"] or "",
            ', '.join(report["src_router"]) if isinstance(report["src_router"], list) else report["src_router"] or "",
            ', '.join(report["dst_router"]) if isinstance(report["dst_router"], list) else report["dst_router"] or "",
            ', '.join(report["Status"]) if isinstance(report["Status"], list) else report["Status"] or ""
        ]
        rows_to_insert.append(row)
        
    try:
        if rows_to_insert:
            sheet.append_rows(rows_to_insert)
        print("Отчет успешно сохранен в Google Таблицу.")
    except gspread.exceptions.APIError as e:
        print(f"Ошибка при добавлении строк: {e}")

# Сохранение отчета в Google Sheets
save_to_google_sheets(report_data)
