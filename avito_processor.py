# processors/avito_processor.py
import sqlite3
import re
import json
from typing import List, Dict, Optional
from datetime import datetime
import os
from bs4 import BeautifulSoup


class AvitoItem:
    """Класс для хранения данных одного объявления"""

    def __init__(self):
        self.title = ""
        self.price = ""
        self.bail = ""
        self.tax = ""
        self.services = ""
        self.address = ""
        self.desc = ""
        self.images = []
        self.link = ""
        # Убрали parsed_date

    def to_dict(self) -> Dict:
        """Преобразует объект в словарь"""
        return {
            'title': self.title,
            'price': self.price,
            'bail': self.bail,
            'tax': self.tax,
            'services': self.services,
            'address': self.address,
            'desc': self.desc,
            'images': json.dumps(self.images, ensure_ascii=False),
            'link': self.link,
            # Убрали parsed_date
        }

    def __str__(self) -> str:
        return f"AvitoItem(title={self.title}, price={self.price}, address={self.address})"


class AvitoDatabase:
    """Класс для работы с базой данных SQLite"""

    def __init__(self, db_path: str = "database/avito_apartments.db"):
        # Создаем папку database если она не существует
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Инициализирует базу данных и создает таблицы"""
        # Убеждаемся, что папка существует
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS apartments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                price TEXT NOT NULL,
                bail TEXT,
                tax TEXT,
                services TEXT,
                address TEXT,
                desc TEXT,
                images TEXT,
                link TEXT UNIQUE
                -- Убрали parsed_date и created_at
            )
        ''')

        # Создаем индекс для быстрого поиска по ссылке
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_link ON apartments(link)
        ''')

        conn.commit()
        conn.close()

    def save_apartment(self, item: AvitoItem) -> bool:
        """Сохраняет объявление в базу данных"""
        try:
            # Убеждаемся, что папка существует
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO apartments 
                (title, price, bail, tax, services, address, desc, images, link)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.title, item.price, item.bail, item.tax, item.services,
                item.address, item.desc, json.dumps(item.images, ensure_ascii=False),
                item.link
                # Убрали parsed_date
            ))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка при сохранении в базу данных: {e}")
            return False

    def apartment_exists(self, link: str) -> bool:
        """Проверяет, существует ли объявление с данной ссылкой"""
        # Убеждаемся, что папка существует
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT 1 FROM apartments WHERE link = ?', (link,))
        exists = cursor.fetchone() is not None

        conn.close()
        return exists


class AvitoHTMLParser:
    """Класс для парсинга HTML страницы Avito"""

    def __init__(self):
        self.soup = None

    def parse_html(self, html: str) -> List[AvitoItem]:
        """Парсит HTML и возвращает список объявлений"""
        self.soup = BeautifulSoup(html, 'html.parser')
        items = []

        # Ищем все карточки объявлений
        item_selectors = [
            '[data-marker="item"]',
            '.iva-item-root',
            '.items-items-kAJAg .iva-item-root',
            '.item-item',
            '.js-catalog-item'
        ]

        for selector in item_selectors:
            item_elements = self.soup.select(selector)
            if item_elements:
                print(f"Найдено {len(item_elements)} объявлений с селектором: {selector}")
                for item_element in item_elements:
                    avito_item = self.parse_single_item(item_element)
                    if avito_item and avito_item.title:  # Проверяем, что объявление валидное
                        items.append(avito_item)
                break

        return items

    def parse_single_item(self, item_element) -> Optional[AvitoItem]:
        """Парсит одно объявление"""
        item = AvitoItem()

        try:
            # Парсим заголовок
            item.title = self.parse_title(item_element)

            # Парсим цену
            item.price = self.parse_price(item_element)

            # Парсим залог
            item.bail = self.parse_bail(item_element)

            # Парсим комиссию
            item.tax = self.parse_tax(item_element)

            # Парсим услуги (ЖКУ)
            item.services = self.parse_services(item_element)

            # Парсим адрес
            item.address = self.parse_address(item_element)

            # Парсим описание
            item.desc = self.parse_description(item_element)

            # Парсим изображения (только первые 3)
            item.images = self.parse_images(item_element)

            # Парсим ссылку
            item.link = self.parse_link(item_element)

            return item

        except Exception as e:
            print(f"Ошибка при парсинге объявления: {e}")
            return None

    def parse_title(self, item_element) -> str:
        """Парсит заголовок объявления"""
        selectors = [
            '[data-marker="item-title"]',
            '.iva-item-titleStep',
            '.title-root',
            'h3 a',
            '.iva-item-title'
        ]

        for selector in selectors:
            element = item_element.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title:
                    return title

        return ""

    def parse_price(self, item_element) -> str:
        """Парсит цену"""
        selectors = [
            '[data-marker="item-price"]',
            '.price-price',
            '.iva-item-priceStep',
            '.js-item-price'
        ]

        for selector in selectors:
            element = item_element.select_one(selector)
            if element:
                price_text = element.get_text(strip=True)
                # Очищаем цену от лишних символов
                price_text = re.sub(r'\s+', ' ', price_text)
                if '₽' in price_text or 'руб' in price_text.lower():
                    return price_text

        return ""

    def parse_bail(self, item_element) -> str:
        """Парсит залог"""
        # Ищем текст, содержащий слова связанные с залогом
        text_content = item_element.get_text()
        bail_patterns = [
            r'залог[^.]*?(\d[\d\s]*?₽)',
            r'депозит[^.]*?(\d[\d\s]*?₽)',
            r'обеспечение[^.]*?(\d[\d\s]*?₽)'
        ]

        for pattern in bail_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                return f"Залог {match.group(1)}"

        return ""

    def parse_tax(self, item_element) -> str:
        """Парсит комиссию"""
        text_content = item_element.get_text()
        tax_patterns = [
            r'комиссия[^.]*?(\d+%)',
            r'вознаграждение[^.]*?(\d+%)',
            r'% комиссии'
        ]

        for pattern in tax_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                return f"Комиссия {match.group(1)}"

        return ""

    def parse_services(self, item_element) -> str:
        """Парсит информацию о коммунальных услугах"""
        text_content = item_element.get_text()
        service_patterns = [
            r'ЖКУ[^.]*включены',
            r'коммуналка[^.]*включена',
            r'ком\. услуги[^.]*включены'
        ]

        for pattern in service_patterns:
            if re.search(pattern, text_content, re.IGNORECASE):
                return "ЖКУ включены"

        return ""

    def parse_address(self, item_element) -> str:
        """Парсит адрес (только адрес, без информации о метро)"""
        selectors = [
            '[data-marker="item-address"]',
            '.iva-item-address',
            '.geo-address',
            '.item-address'
        ]

        for selector in selectors:
            element = item_element.select_one(selector)
            if element:
                # Ищем ссылки с улицей и домом
                street_links = element.select('a[href*="/catalog/houses/"], a[href*="/kvartiry/"]')

                address_parts = []
                for link in street_links[:2]:  # Берем только первые две ссылки (улица и дом)
                    text = link.get_text(strip=True)
                    if text:
                        address_parts.append(text)

                if address_parts:
                    # Объединяем улицу и дом
                    address = ', '.join(address_parts)
                    return re.sub(r'\s+', ' ', address).strip()

                # Если не нашли ссылки, пробуем старый метод как fallback
                full_text = element.get_text()
                if full_text:
                    # Удаляем все после запятой, которая идет после номера дома
                    address_match = re.search(r'^([^,]+(?:,[^,]+)?)', full_text)
                    if address_match:
                        return address_match.group(1).strip()

        return ""

    def parse_description(self, item_element) -> str:
        """Парсит описание"""
        selectors = [
            '[data-marker="item-specific-params"]',
            '.iva-item-description',
            '.item-description',
            '.description-text'
        ]

        for selector in selectors:
            element = item_element.select_one(selector)
            if element:
                desc = element.get_text(strip=True)
                if desc:
                    # Ограничиваем длину описания
                    return desc[:500] + "..." if len(desc) > 500 else desc

        return ""

    def parse_images(self, item_element) -> List[str]:
        """Парсит ссылки на изображения из карусели Avito (только первые 3)"""
        images = []

        # Ищем элементы карусели
        carousel_selectors = [
            '.photo-slider-list-item-r2YDC',
            '[data-marker*="slider-image"]',
            '.photo-slider-item-mbNB3'
        ]

        for selector in carousel_selectors:
            carousel_items = item_element.select(selector)
            for item in carousel_items:
                if len(images) >= 3:
                    break

                # Пробуем найти изображение внутри элемента карусели
                img = item.select_one('img')
                if img:
                    src = (img.get('src') or img.get('data-src'))
                    if src:
                        # Обрабатываем разные форматы ссылок
                        if src.startswith('//'):
                            src = 'https:' + src
                        elif src.startswith('/'):
                            src = 'https://www.avito.ru' + src

                        if src.startswith('http') and src not in images:
                            images.append(src)

                # Если не нашли в img, пробуем из data-marker атрибута
                if not img:
                    data_marker = item.get('data-marker', '')
                    if 'slider-image/image-' in data_marker:
                        src = data_marker.replace('slider-image/image-', '')
                        if src and src not in images:
                            if src.startswith('//'):
                                src = 'https:' + src
                            images.append(src)

        # Fallback: обычный поиск изображений
        if not images:
            img_elements = item_element.select('img[src*="avito.st"]')
            for img in img_elements:
                if len(images) >= 3:
                    break
                src = img.get('src')
                if src and src not in images:
                    if src.startswith('//'):
                        src = 'https:' + src
                    images.append(src)

        return images[:3]

    def parse_link(self, item_element) -> str:
        """Парсит ссылку на объявление"""
        selectors = [
            'a[data-marker="item-title"]',
            'a.iva-item-title',
            'a.link-link',
            'a[href*="/ufa/kvartiry/"]'
        ]

        for selector in selectors:
            element = item_element.select_one(selector)
            if element:
                href = element.get('href')
                if href:
                    if href.startswith('//'):
                        return 'https:' + href
                    elif href.startswith('/'):
                        return 'https://www.avito.ru' + href
                    else:
                        return href

        return ""


class AvitoProcessor:
    """Основной класс процессора для обработки HTML Avito"""

    def __init__(self, db_path: str = "database/avito_apartments.db"):
        # Создаем папку database если она не существует
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.parser = AvitoHTMLParser()
        self.database = AvitoDatabase(db_path)
        self.stats = {
            'total_processed': 0,
            'new_items': 0,
            'existing_items': 0,
            'errors': 0
        }

    def process_html(self, html: str, url: str, output_to_console: bool = True) -> Dict:
        """
        Основной метод обработки HTML
        Возвращает статистику обработки
        """
        print("🔍 Начинаем парсинг HTML страницы Avito...")

        try:
            # Парсим HTML
            items = self.parser.parse_html(html)
            print(f"📊 Найдено {len(items)} объявлений")

            # Сохраняем в базу данных
            for item in items:
                self.stats['total_processed'] += 1

                if not item.link:
                    self.stats['errors'] += 1
                    continue

                # Убрали отладочную информацию о каждом объявлении
                if self.database.apartment_exists(item.link):
                    self.stats['existing_items'] += 1
                else:
                    if self.database.save_apartment(item):
                        self.stats['new_items'] += 1
                    else:
                        self.stats['errors'] += 1

            # Выводим статистику
            if output_to_console:
                self.print_statistics()

            return self.stats

        except Exception as e:
            print(f"❌ Ошибка при обработке HTML: {e}")
            self.stats['errors'] += 1
            return self.stats

    def print_statistics(self):
        """Выводит статистику обработки"""
        print("\n📈 СТАТИСТИКА ОБРАБОТКИ:")
        print(f"   Всего обработано: {self.stats['total_processed']}")
        print(f"   Новых объявлений: {self.stats['new_items']}")
        print(f"   Существующих: {self.stats['existing_items']}")
        print(f"   Ошибок: {self.stats['errors']}")

    def get_recent_items(self, limit: int = 10) -> List[Dict]:
        """Возвращает последние добавленные объявления"""
        conn = sqlite3.connect(self.database.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT title, price, address, images, link
            FROM apartments 
            ORDER BY id DESC 
            LIMIT ?
        ''', (limit,))

        items = []
        for row in cursor.fetchall():
            images_data = json.loads(row[3]) if row[3] else []
            items.append({
                'title': row[0],
                'price': row[1],
                'address': row[2],
                'images_count': len(images_data),
                'link': row[4]
            })

        conn.close()
        return items


# Функция для настройки процессора
def setup_avito_processor():
    """Создает и настраивает процессор Avito"""
    return AvitoProcessor()
