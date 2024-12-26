import requests
import pandas as pd
import uuid  # Импортируем модуль uuid для генерации уникальных идентификаторов

# Настройки
environment = "staging"  # staging/prod/dev
last_amount = 50  # Сумма обмена в USD
enable_exceptions = False  # Включить/выключить исключения
enable_filters = False  # Включить/выключить фильтры

# Загрузка исключений из файла
if enable_exceptions:
    exceptions_df = pd.read_excel(
        "C:\\Users\\unton\\Downloads\\Trading_bot\\exceptions.xlsx"
    )
    exceptions = set(
        zip(
            exceptions_df["from_symbol"],
            exceptions_df["to_symbol"],
            exceptions_df["from_chain_id"],
            exceptions_df["to_chain_id"],
        )
    )

# Определение URL для первого запроса
url_cryptos = f"https://crypto.{environment}.swaps.io/api/v0/cryptos"

# Список для хранения отчетных данных
report_data = []

# Отправка первого GET-запроса
response_cryptos = requests.get(url_cryptos)

# Проверка успешности запроса
if response_cryptos.status_code == 200:
    data = response_cryptos.json()
    cryptos = data.get("cryptos", [])

    if isinstance(cryptos, list) and len(cryptos) >= 4:
        for i in range(len(cryptos)):
            from_block = cryptos[i]
            from_address = from_block.get("address")
            from_chain_id = from_block.get("chain_id")
            from_symbol = from_block.get("symbol")
            from_decimals = from_block.get("decimals")

            # Проверка на допустимые значения (первый фильтр)
            if enable_filters and from_chain_id not in [
                "43114",
                "42161",
                "60808",
                "100",
                "668467",
            ]:
                continue

            for j in range(len(cryptos)):
                if i == j:
                    continue
                to_block = cryptos[j]
                to_address = to_block.get("address")
                to_chain_id = to_block.get("chain_id")
                to_symbol = to_block.get("symbol")
                to_decimals = to_block.get("decimals")

                # Проверка на допустимые значения (второй фильтр)
                if enable_filters and to_chain_id not in [
                    "43114",
                    "42161",
                    "60808",
                    "100",
                    "668467",
                ]:
                    if from_chain_id != "668467":
                        continue

                # Проверка на наличие связки в исключениях
                if (
                    enable_exceptions
                    and (from_symbol, to_symbol, from_chain_id, to_chain_id)
                    in exceptions
                ):
                    print(
                        f"Пропускаем связку: {from_chain_id}, {from_symbol} -> {to_chain_id}, {to_symbol}"
                    )
                    continue

                if from_address and from_chain_id and to_address and to_chain_id:
                    # Запрос на получение цен
                    url_price_map = (
                        f"https://meta2.{environment}.swaps.io/api/price/map"
                    )
                    response_price_map = requests.get(url_price_map)

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

                                # Генерация случайного trace id
                                trace_id = str(uuid.uuid4())

                                # Формирование URL для второго запроса
                                url_quote = (
                                    f"https://api.{environment}.swaps.io/api/v0/quote"
                                )
                                params = {
                                    "from_chain_id": from_chain_id,
                                    "from_token_address": from_address,
                                    "from_amount": str(adjusted_value),
                                    "to_chain_id": to_chain_id,
                                    "to_token_address": to_address,
                                }
                                headers = {
                                    "X-Request-ID": trace_id  # Добавляем trace id в заголовок
                                }

                                # Переменные для повторных попыток
                                max_attempts = 3
                                attempt = 0
                                success = False

                                while attempt < max_attempts and not success:
                                    response_quote = requests.get(
                                        url_quote, params=params, headers=headers
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
                                        to_to_amount = quote_data.get("to_amount")

                                        if to_to_amount is not None:
                                            to_to_amount = float(to_to_amount)
                                            to_last_amount = (
                                                to_to_amount * (to_value / 10**8)
                                            ) / (10**to_decimals)

                                            lower_bound = last_amount * 0.85
                                            upper_bound = last_amount * 1.15

                                            # считаем просадку
                                            drawdown = 100 - (
                                                to_last_amount / last_amount * 100
                                            )

                                            # Проверка всех условий
                                            status_messages = []

                                            if not (
                                                lower_bound
                                                <= to_last_amount
                                                <= upper_bound
                                            ):
                                                status_messages.append(
                                                    f"To Amount ({to_last_amount}) not in bounds ({lower_bound}, {upper_bound})"
                                                )

                                            if (
                                                quote_data.get("from_chain_id")
                                                != from_chain_id
                                            ):
                                                status_messages.append(
                                                    f'From Chain ID mismatch: {quote_data.get("from_chain_id")} != {from_chain_id}'
                                                )

                                            if quote_data.get("from_amount") != str(
                                                adjusted_value
                                            ):
                                                status_messages.append(
                                                    f'From Amount mismatch: {quote_data.get("from_amount")} != {adjusted_value}'
                                                )

                                            if (
                                                quote_data.get("to_chain_id")
                                                != to_chain_id
                                            ):
                                                status_messages.append(
                                                    f'To Chain ID mismatch: {quote_data.get("to_chain_id")} != {to_chain_id}'
                                                )

                                            if (
                                                quote_data.get("to_token_address")
                                                != to_address
                                            ):
                                                status_messages.append(
                                                    f'To Token Address mismatch: {quote_data.get("to_token_address")} != {to_address}'
                                                )

                                            if status_messages:
                                                report_data.append(
                                                    {
                                                        "Trace ID": trace_id,
                                                        "From Symbol": from_symbol,
                                                        "To Symbol": to_symbol,
                                                        "From Amount (USD)": last_amount,
                                                        "To Amount (USD)": to_last_amount,
                                                        "drawdown (%)": drawdown,
                                                        "From Chain ID": from_chain_id,
                                                        "To Chain ID": to_chain_id,
                                                        "Status": "Conditions not met: "
                                                        + ", ".join(status_messages),
                                                    }
                                                )
                                            else:
                                                report_data.append(
                                                    {
                                                        "Trace ID": trace_id,
                                                        "From Symbol": from_symbol,
                                                        "To Symbol": to_symbol,
                                                        "From Amount (USD)": last_amount,
                                                        "To Amount (USD)": to_last_amount,
                                                        "drawdown (%)": drawdown,
                                                        "From Chain ID": from_chain_id,
                                                        "To Chain ID": to_chain_id,
                                                        "Status": "Success",
                                                    }
                                                )
                                        else:
                                            report_data.append(
                                                {
                                                    "Trace ID": trace_id,
                                                    "From Symbol": from_symbol,
                                                    "To Symbol": to_symbol,
                                                    "From Amount (USD)": last_amount,
                                                    "To Amount (USD)": None,
                                                    "drawdown (%)": None,
                                                    "From Chain ID": from_chain_id,
                                                    "To Chain ID": to_chain_id,
                                                    "Status": "to_amount is None",
                                                }
                                            )
                                        success = True
                                    else:
                                        print(
                                            from_chain_id,
                                            from_symbol,
                                            "->",
                                            to_chain_id,
                                            to_symbol,
                                            f"attempts: {response_quote.status_code} - {response_quote.text}",
                                        )
                                        attempt += 1
                                        if attempt == max_attempts:
                                            report_data.append(
                                                {
                                                    "Trace ID": trace_id,
                                                    "From Symbol": from_symbol,
                                                    "To Symbol": to_symbol,
                                                    "From Amount (USD)": last_amount,
                                                    "To Amount (USD)": None,
                                                    "drawdown (%)": None,
                                                    "From Chain ID": from_chain_id,
                                                    "To Chain ID": to_chain_id,
                                                    "Status": f"attempts: {response_quote.status_code} - {response_quote.text}",
                                                }
                                            )
                            else:
                                report_data.append(
                                    {
                                        "Trace ID": None,
                                        "From Symbol": from_symbol,
                                        "To Symbol": to_symbol,
                                        "From Amount (USD)": last_amount,
                                        "To Amount (USD)": None,
                                        "drawdown (%)": None,
                                        "From Chain ID": from_chain_id,
                                        "To Chain ID": to_chain_id,
                                        "Status": "Value is None (/api/price/map)",
                                    }
                                )
                        else:
                            report_data.append(
                                {
                                    "Trace ID": None,
                                    "From Symbol": from_symbol,
                                    "To Symbol": to_symbol,
                                    "From Amount (USD)": last_amount,
                                    "To Amount (USD)": None,
                                    "drawdown (%)": None,
                                    "From Chain ID": from_chain_id,
                                    "To Chain ID": to_chain_id,
                                    "Status": "Price not found",
                                }
                            )
                else:
                    report_data.append(
                        {
                            "Trace ID": None,
                            "From Symbol": from_symbol,
                            "To Symbol": to_symbol,
                            "From Amount (USD)": None,
                            "To Amount (USD)": None,
                            "drawdown (%)": None,
                            "From Chain ID": from_chain_id,
                            "To Chain ID": to_chain_id,
                            "Status": "Invalid addresses or chain IDs",
                        }
                    )
    else:
        print("Недостаточно криптовалют для обработки.")
else:
    print(
        f"Ошибка при получении криптовалют: {response_cryptos.status_code} - {response_cryptos.text}"
    )

# Создание DataFrame и сохранение в Excel
df = pd.DataFrame(report_data)
df.to_excel("crypto_report.xlsx", index=False)

print("Отчет успешно сохранен в файл crypto_report.xlsx.")