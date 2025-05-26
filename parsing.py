import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Загружаем CSV-файл
INPUT_FILE = "Ссылки для парсинга - Лист1.csv"
SPREADSHEET_ID = "1zv1sDWeKZu0NYHzNg6PC71_RwjzahNvTI905LYJue_E"

# Сайты, которые нужно исключить
EXCLUDED_DOMAINS = ["ozon", "stomatorg"]

# Функция для определения домена сайта
def get_domain(url):
    return urlparse(url).netloc.replace("www.", "")

# Специальный парсер для сайта axiomadent.ru
def parse_axiomadent_price(html):
    soup = BeautifulSoup(html, "html.parser")
    price_block = soup.find("div", class_="product__price--orig")
    if price_block:
        text = price_block.get_text(strip=True)
        match = re.search(r"(\d[\d\s]*\d)\s?(₽|руб)?", text)
        if match:
            return int(match.group(1).replace(" ", ""))
    return None

# Специальный парсер для сайта diamed.pro
def parse_diamed_price(html):
    soup = BeautifulSoup(html, "html.parser")
    price_span = soup.find("span", class_="ty-price-num")
    if price_span:
        text = price_span.get_text(strip=True)
        match = re.search(r"(\d[\d\s]*\d)", text)
        if match:
            return int(match.group(1).replace("\xa0", "").replace(" ", ""))
    return None

# Универсальный парсер цены
def extract_price_from_html(html, domain):
    if "axiomadent.ru" in domain:
        return parse_axiomadent_price(html)
    if "diamed.pro" in domain:
        return parse_diamed_price(html)

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=' ', strip=True)
    match = re.search(r"(\d[\d\s]*\d)\s?(₽|руб)", text)
    if match:
        price = match.group(1).replace(" ", "")
        return int(price)
    return None

# Парсинг цен по ссылкам
def parse_prices_from_csv(input_csv):
    df = pd.read_csv(input_csv)

    urls = []
    products = []
    for col in df.columns[1:]:
        for i in range(len(df)):
            url = df.iloc[i][col]
            if isinstance(url, str) and url.startswith("http"):
                if not any(ex in url.lower() for ex in EXCLUDED_DOMAINS):
                    urls.append(url)
                    products.append(col)

    results = []
    for product, url in zip(products, urls):
        domain = get_domain(url)
        try:
            response = requests.get(url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            })
            response.raise_for_status()
            price = extract_price_from_html(response.text, domain)
            results.append([domain, product, price if price else "Не найдено", url])
        except Exception as e:
            results.append([domain, product, f"Ошибка: {str(e)}", url])
        # Удалили time.sleep(1) для ускорения работы на PythonAnywhere

    return results

# Сохранение в Google Sheets
def save_to_google_sheet(data):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1

    # Очистить таблицу перед записью
    sheet.clear()

    # Записать всё одной операцией
    sheet.update(values=[["Сайт", "Товар", "Цена (₽)", "Ссылка"]] + data, range_name='A1')

if __name__ == "__main__":
    print("Начинаем парсинг...")
    result_data = parse_prices_from_csv(INPUT_FILE)
    print("Сохраняем в Google Таблицу...")
    save_to_google_sheet(result_data)
    print("Готово! Данные обновлены в Google Таблице.")
