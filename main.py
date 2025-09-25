# main.py
import os
import json
import random
import asyncio
from typing import Dict, Optional
from dotenv import load_dotenv
from datetime import datetime
import hashlib
from avito_processor import AvitoProcessor, setup_avito_processor

from playwright.async_api import async_playwright, BrowserContext, TimeoutError

# Импортируем кастомные заголовки из services
from services.headers import CUSTOM_HEADERS

# Загружаем .env файл
load_dotenv()

# Пути к файлам
USER_AGENTS_FILE = 'services/user_agent_pc.txt'
COOKIES_FILE = 'cookie.json'
TARGET_URL = os.getenv('TARGET_URL')
TRASH_DIR = 'trash'

if not TARGET_URL:
    raise ValueError("Константа TARGET_URL не задана в файле .env")

os.makedirs(TRASH_DIR, exist_ok=True)


class FileManager:
    """
    Класс для управления файлами: сохранение HTML в папку trash
    """

    @staticmethod
    def generate_filename(url: str, suffix: str = "") -> str:
        """
        Генерирует уникальное имя файла на основе URL и временной метки
        """
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        clean_url = url.replace('https://', '').replace('http://', '')
        clean_url = ''.join(c if c.isalnum() or c in ('-', '_', '.') else '_' for c in clean_url)
        clean_url = clean_url[:50]

        filename = f"{timestamp}_{url_hash}_{clean_url}"
        if suffix:
            filename += f"_{suffix}"
        filename += ".html"

        return os.path.join(TRASH_DIR, filename)

    @staticmethod
    def save_html_to_file(html: str, url: str, suffix: str = "") -> str:
        """
        Сохраняет HTML в файл и возвращает путь к файлу
        """
        filename = FileManager.generate_filename(url, suffix)

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"💾 HTML сохранен в файл: {filename}")
            return filename
        except Exception as e:
            print(f"❌ Ошибка при сохранении HTML в файл: {e}")
            return ""

    @staticmethod
    def cleanup_old_files(max_files: int = 50):
        """
        Удаляет старые файлы, если их больше max_files
        """
        try:
            if not os.path.exists(TRASH_DIR):
                return

            files = [os.path.join(TRASH_DIR, f) for f in os.listdir(TRASH_DIR)
                     if f.endswith('.html')]
            files.sort(key=os.path.getmtime)

            if len(files) > max_files:
                files_to_delete = files[:len(files) - max_files]
                for file_path in files_to_delete:
                    try:
                        os.remove(file_path)
                        print(f"🗑️ Удален старый файл: {os.path.basename(file_path)}")
                    except Exception as e:
                        print(f"⚠ Ошибка при удалении файла {file_path}: {e}")
        except Exception as e:
            print(f"⚠ Ошибка при очистке старых файлов: {e}")


class RandomHeaders:
    def __init__(self):
        if not os.path.exists(USER_AGENTS_FILE):
            raise FileNotFoundError(f"Файл {USER_AGENTS_FILE} не найден.")

        with open(USER_AGENTS_FILE, 'r', encoding='utf-8') as f:
            self.user_agents = [line.strip() for line in f.readlines() if line.strip()]

        if not self.user_agents:
            raise ValueError("Список user-агентов пуст.")

    def get_random_headers(self) -> Dict[str, str]:
        headers = CUSTOM_HEADERS.copy()
        headers['user-agent'] = random.choice(self.user_agents)
        return headers


class BrowserSession:
    def __init__(self, headless: bool = True):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.headless = headless
        self.file_manager = FileManager()

    async def apply_stealth_measures(self, page):
        await page.add_init_script("""
            () => {
                delete navigator.__proto__.webdriver;
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ru-RU', 'ru', 'en-US', 'en'],
                });
            }
        """)

    async def start(self):
        self.playwright = await async_playwright().start()

        launch_options = {
            'headless': self.headless,
            'args': [
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-extensions',
                '--disable-plugins-discovery',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-field-trial-config',
                '--disable-automation-controller',
            ]
        }

        self.browser = await self.playwright.chromium.launch(**launch_options)

        storage_state = COOKIES_FILE if os.path.exists(COOKIES_FILE) else None

        context_options = {
            'viewport': {'width': 1920, 'height': 1080},
            'locale': 'ru-RU',
            'timezone_id': 'Europe/Moscow',
            'permissions': ['geolocation'],
            'geolocation': {'latitude': 55.7558, 'longitude': 37.6173},
            'extra_http_headers': CUSTOM_HEADERS.copy(),
            'user_agent': random.choice(RandomHeaders().user_agents) if os.path.exists(USER_AGENTS_FILE) else None,
        }

        if storage_state:
            context_options['storage_state'] = storage_state

        self.context = await self.browser.new_context(**context_options)
        self.page = await self.context.new_page()

        await self.apply_stealth_measures(self.page)

        if not os.path.exists(COOKIES_FILE):
            print(f"Файл {COOKIES_FILE} не найден. Запускаем браузер для ручного логина.")
            await self.close()

            launch_options['headless'] = False
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(**launch_options)
            self.context = await self.browser.new_context(**context_options)
            self.page = await self.context.new_page()
            await self.apply_stealth_measures(self.page)

            print("Переходим на Avito.ru для логина...")
            await self.safe_goto('https://www.avito.ru')

            input("После успешного логина нажмите Enter для сохранения кук...")
            await self.save_cookies()
            print(f"Куки сохранены в {COOKIES_FILE}")

    async def safe_goto(self, url: str, max_retries: int = 3) -> bool:
        for attempt in range(max_retries):
            try:
                print(f"Попытка {attempt + 1}/{max_retries} перехода на {url}")
                await self.page.goto(url, wait_until='domcontentloaded', timeout=60000)
                await asyncio.sleep(3)

                page_title = await self.page.title()
                page_url = self.page.url
                print(f"Заголовок страницы: {page_title}")
                print(f"Текущий URL: {page_url}")

                if any(keyword in page_url.lower() or keyword in page_title.lower()
                       for keyword in ['captcha', 'challenge', 'block', 'access denied']):
                    print("Обнаружена капча или блокировка. Пробуем обойти...")
                    await asyncio.sleep(10)
                    continue

                try:
                    screenshot_path = os.path.join(TRASH_DIR, f"debug_attempt_{attempt + 1}.png")
                    await self.page.screenshot(path=screenshot_path)
                    print(f"📸 Скриншот сохранен: {screenshot_path}")
                except:
                    pass

                return True

            except TimeoutError:
                print(f"Таймаут при попытке {attempt + 1}. Пробуем снова...")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                else:
                    raise
            except Exception as e:
                print(f"Ошибка при попытке {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    try:
                        await self.page.close()
                        self.page = await self.context.new_page()
                        await self.apply_stealth_measures(self.page)
                    except:
                        pass
                    continue
                else:
                    raise
        return False

    async def set_headers(self, headers: Dict[str, str]):
        await self.context.set_extra_http_headers(headers)

    async def goto_and_wait(self, url: str) -> None:
        success = await self.safe_goto(url)

        if not success:
            raise Exception(f"Не удалось загрузить страницу {url} после нескольких попыток")

        try:
            await self.page.wait_for_load_state('load', timeout=30000)

            avito_selectors = [
                'div[data-marker="item"]',
                '.items-items-kAJAg',
                '[data-marker="catalog-serp"]',
                '.item-item',
                '.iva-item-root',
                '.index-content',
                '.page-body'
            ]

            for selector in avito_selectors:
                try:
                    await self.page.wait_for_selector(selector, timeout=10000)
                    print(f"✓ Найден элемент: {selector}")
                    break
                except:
                    continue
            else:
                print("⚠ Не найдены стандартные элементы Avito, но страница загружена")

            await self.page.evaluate("window.scrollTo(0, 300)")
            await asyncio.sleep(1)

            print(f"✓ Страница {url} успешно загружена и проверена")

        except Exception as e:
            print(f"⚠ Предупреждение при проверке страницы: {e}")

    async def get_page_content(self) -> str:
        return await self.page.content()

    async def save_cookies(self):
        await self.context.storage_state(path=COOKIES_FILE)

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()


class PageProcessor:
    """
    Класс для обработки содержимого страницы.
    Теперь ТОЛЬКО обрабатывает HTML, но не сохраняет его.
    """

    def __init__(self):
        self.file_manager = FileManager()

    def process_html(self, html: str, url: str, output_to_console: bool = True, callback: Optional[callable] = None):
        """
        Обрабатывает HTML: выводит в консоль и вызывает callback.
        Сохранение в файл теперь происходит в основном классе.
        """
        if output_to_console:
            print("=== ПРЕВЬЮ СТРАНИЦЫ (первые 500 символов) ===")
            preview = html[:500] + "..." if len(html) > 500 else html
            print(preview)
            print("=== КОНЕЦ ПРЕВЬЮ ===")

        if callback:
            # Передаем только HTML, без пути к файлу
            callback(html)


class AvitoParser:
    """
    Основной класс парсера объявлений о посуточной аренде на Avito.
    """

    def __init__(self, headless: bool = True):
        self.target_url = TARGET_URL
        self.headers_manager = RandomHeaders()
        self.session = BrowserSession(headless=headless)
        self.processor = PageProcessor()
        self.file_manager = FileManager()

    async def start(self):
        """Запускает сессию браузера."""
        await self.session.start()
        await self.session.set_headers(self.headers_manager.get_random_headers())
        self.file_manager.cleanup_old_files(max_files=20)

    async def fetch_target_page(self) -> str:
        """Загружает целевую страницу и возвращает HTML."""
        await self.session.goto_and_wait(self.target_url)
        html = await self.session.get_page_content()
        await self.session.save_cookies()
        return html

    async def parse_target(self, callback: Optional[callable] = None):
        """
        Парсит целевую страницу.
        Сохраняет HTML в файл ТОЛЬКО ОДИН РАЗ.
        """
        # Получаем HTML
        html = await self.fetch_target_page()

        # Сохраняем HTML в файл
        saved_file_path = self.file_manager.save_html_to_file(html, self.target_url, "parsed")

        # Обрабатываем HTML
        self.processor.process_html(html, self.target_url, callback=callback)

        return saved_file_path  # Возвращаем путь к сохраненному файлу

    async def parse_with_processor(self):
        """Парсит страницу с использованием нового процессора"""
        processor = setup_avito_processor()

        def processor_callback(html):
            stats = processor.process_html(html, self.target_url)
            print(f"✅ Обработка завершена. Новых объявлений: {stats['new_items']}")

        await self.parse_target(callback=processor_callback)

    async def close(self):
        """Закрывает сессию браузера."""
        await self.session.close()


# Асинхронный main
async def main():
    print("🚀 Запуск улучшенного парсера Avito...")

    parser = AvitoParser(headless=False)

    try:
        await parser.start()
        await parser.parse_with_processor()  # Используем новый метод
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await parser.close()


if __name__ == "__main__":
    asyncio.run(main())
