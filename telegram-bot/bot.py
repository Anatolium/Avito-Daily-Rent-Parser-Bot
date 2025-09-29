import os
import sqlite3
import json
import logging
from typing import Dict, List
from telebot import TeleBot, types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import asyncio
import threading
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла в корне проекта
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AvitoTelegramBot:
    def __init__(self, token: str = None, db_path: str = None):
        self.token = token or os.getenv('BOT_TOKEN')
        if not self.token:
            raise ValueError("BOT_TOKEN не найден. Убедитесь, что он установлен в .env файле")

        # Автоматически определяем путь к БД
        if db_path is None:
            bot_dir = os.path.dirname(__file__)
            project_root = os.path.join(bot_dir, '..')
            db_path = os.path.join(project_root, 'database', 'avito_apartments.db')
            db_path = os.path.abspath(db_path)

        self.bot = TeleBot(self.token)
        self.db_path = db_path
        self.user_states = {}
        self.last_parsing_stats = {
            'status': 'not_run',
            'total_processed': 0,
            'errors': 0
        }

        self.setup_handlers()

    def setup_handlers(self):
        @self.bot.message_handler(commands=['start', 'help', 'menu'])
        def send_welcome(message):
            self.show_main_menu(message.chat.id)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('menu_'))
        def handle_menu_callback(call):
            action = call.data.replace('menu_', '')

            if action == 'parser':
                self.start_parser(call)
            elif action == 'status':
                self.show_parser_status(call)
            elif action == 'journal':
                self.show_journal(call, offset=0)
            elif action == 'settings':
                self.show_settings(call)

            self.bot.answer_callback_query(call.id)

        @self.bot.callback_query_handler(func=lambda call: call.data == 'main_menu')
        def handle_back_to_menu(call):
            self.show_main_menu(call.message.chat.id, "🏠 *Главное меню*")
            self.bot.answer_callback_query(call.id)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith("journal_"))
        def handle_journal_pagination(call):
            offset = int(call.data.split("_")[1])
            self.show_journal(call, offset)
            self.bot.answer_callback_query(call.id)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith("object_"))
        def handle_object_click(call):
            self.handle_object_callback(call)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith("settings_"))
        def handle_settings_click(call):
            if call.data == "settings_notify":
                self.bot.answer_callback_query(call.id, "Настройки уведомлений в разработке")
            elif call.data == "settings_interval":
                self.bot.answer_callback_query(call.id, "Настройки интервала в разработке")

    def show_main_menu(self, chat_id: int, message_text: str = None):
        markup = InlineKeyboardMarkup(row_width=2)

        btn_parser = InlineKeyboardButton("🚀 Запустить парсер", callback_data="menu_parser")
        btn_status = InlineKeyboardButton("📊 Статус парсера", callback_data="menu_status")
        btn_journal = InlineKeyboardButton("📋 Журнал объектов", callback_data="menu_journal")
        btn_settings = InlineKeyboardButton("⚙️ Настройки", callback_data="menu_settings")

        markup.row(btn_parser, btn_status)
        markup.row(btn_journal, btn_settings)

        text = message_text or "🏠 *Управление парсером Avito*\n\nВыберите действие:"

        self.bot.send_message(chat_id, text, reply_markup=markup, parse_mode='Markdown')

    def start_parser(self, call):
        chat_id = call.message.chat.id
        msg = self.bot.send_message(chat_id, "🔄 *Запуск парсера...*", parse_mode='Markdown')

        def run_parser():
            try:
                import sys
                project_root = os.path.join(os.path.dirname(__file__), '..')
                sys.path.append(project_root)
                os.chdir(project_root)

                from main import main as run_avito_parser

                # Запускаем парсер
                result = asyncio.run(run_avito_parser())

                # Сохраняем статистику
                if result and isinstance(result, dict):
                    self.last_parsing_stats = {
                        'status': result.get('status', 'unknown'),
                        'total_processed': result.get('processed', 0),
                        'errors': result.get('errors', 0)
                    }
                else:
                    # Если результат не получен, используем данные из БД
                    total_before = getattr(self, '_objects_before_parsing', 0)
                    total_after = self.get_total_objects()
                    new_objects = total_after - total_before

                    self.last_parsing_stats = {
                        'status': 'completed',
                        'total_processed': new_objects,
                        'errors': 0
                    }

                # Показываем результат
                result_text = (
                    f"✅ *Парсинг завершен!*\n\n"
                    f"📊 *Статистика:*\n"
                    f"• Обработано: {self.last_parsing_stats['total_processed']} объявлений\n"
                    f"• Ошибок: {self.last_parsing_stats['errors']}"
                )

                self.bot.edit_message_text(
                    result_text,
                    chat_id=chat_id,
                    message_id=msg.message_id,
                    parse_mode='Markdown'
                )

            except Exception as e:
                logger.error(f"Ошибка при парсинге: {e}")
                self.last_parsing_stats = {
                    'status': 'error',
                    'total_processed': 0,
                    'errors': 1
                }
                self.bot.edit_message_text(
                    f"❌ *Ошибка:* {str(e)}",
                    chat_id=chat_id,
                    message_id=msg.message_id,
                    parse_mode='Markdown'
                )

        # Сохраняем количество объектов до парсинга
        self._objects_before_parsing = self.get_total_objects()

        thread = threading.Thread(target=run_parser)
        thread.daemon = True
        thread.start()

    def show_parser_status(self, call):
        """Показать статус парсера с последней статистикой"""
        chat_id = call.message.chat.id

        # Общая статистика БД
        total_objects = self.get_total_objects()

        last_run_text = ""

        # Статистика последнего запуска
        if self.last_parsing_stats['status'] == 'not_run':
            last_run_text = (
                "• Статус: ❓ Не выполнялся\n"
                "• Обработано: 0 объявлений\n"
                "• Ошибок: 0"
            )
        elif self.last_parsing_stats['status'] == 'success':
            last_run_text = (
                "• Статус: ✅ Успешно\n"
                f"• Обработано: {self.last_parsing_stats['total_processed']} объявлений\n"
                f"• Ошибок: {self.last_parsing_stats['errors']}"
            )
        elif self.last_parsing_stats['status'] == 'completed_no_stats':
            last_run_text = (
                "• Статус: ✅ Завершен\n"
                "• Обработано: данные не получены\n"
                "• Ошибок: 0"
            )
        elif self.last_parsing_stats['status'] == 'error':
            last_run_text = (
                "• Статус: ❌ С ошибкой\n"
                f"• Обработано: {self.last_parsing_stats['total_processed']} объявлений\n"
                f"• Ошибок: {self.last_parsing_stats['errors']}"
            )

        message_text = (
            "📊 *Статус парсера*\n\n"
            "💾 *База данных:*\n"
            f"• Всего объектов: {total_objects}\n\n"
            f"🔄 *Последний запуск:*\n{last_run_text}"
        )

        self.bot.send_message(chat_id, message_text, parse_mode='Markdown')

    def parse_parser_output(self, output: str):
        """Парсит вывод main.py для извлечения статистики"""
        try:
            # Ищем строки со статистикой
            lines = output.split('\n')
            stats = {'total_processed': 0, 'errors': 0}

            for line in lines:
                if 'Всего обработано:' in line:
                    # Извлекаем число из "Всего обработано: 50"
                    parts = line.split(':')
                    if len(parts) > 1:
                        stats['total_processed'] = int(parts[1].strip())

                elif 'Ошибок:' in line:
                    parts = line.split(':')
                    if len(parts) > 1:
                        stats['errors'] = int(parts[1].strip())

            return stats if stats['total_processed'] > 0 else None

        except Exception as e:
            logger.error(f"Ошибка парсинга вывода: {e}")
            return None

    def show_journal(self, call, offset: int = 0, limit: int = 5):
        chat_id = call.message.chat.id
        objects = self.get_recent_objects(offset, limit)

        if not objects:
            self.bot.send_message(chat_id, "📭 *Журнал пуст*\n\nЗапустите парсер для сбора данных.",
                                  parse_mode='Markdown')
            return

        markup = InlineKeyboardMarkup(row_width=1)

        for obj in objects:
            btn_text = f"🏠 {obj['title'][:25]}... - {obj['price']}"
            markup.add(InlineKeyboardButton(btn_text, callback_data=f"object_{obj['id']}"))

        pagination_buttons = []
        total_objects = self.get_total_objects()

        if offset > 0:
            pagination_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"journal_{offset - limit}"))

        if len(objects) == limit and offset + limit < total_objects:
            pagination_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"journal_{offset + limit}"))

        if pagination_buttons:
            markup.row(*pagination_buttons)

        markup.add(InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        self.bot.send_message(
            chat_id,
            f"📋 *Журнал объектов*\n\nСтраница {offset // limit + 1}\nПоказано: {len(objects)} из {total_objects}",
            reply_markup=markup,
            parse_mode='Markdown'
        )

    def handle_object_callback(self, call):
        object_id = int(call.data.split("_")[1])
        obj = self.get_object_by_id(object_id)

        if not obj:
            self.bot.answer_callback_query(call.id, "❌ Объект не найден")
            return

        message_text = (
            f"🏠 *{obj['title']}*\n\n"
            f"💰 *Цена:* {obj['price']}\n"
            f"📍 *Адрес:* {obj['address']}\n"
            f"📝 *Описание:* {obj['desc'][:200]}..."
        )

        images = json.loads(obj['images']) if obj['images'] else []

        markup = InlineKeyboardMarkup()
        if obj['link']:
            markup.add(InlineKeyboardButton("🌐 Открыть на Avito", url=obj['link']))
        markup.add(InlineKeyboardButton("📋 К журналу", callback_data="journal_0"))
        markup.add(InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        if images:
            try:
                self.bot.send_photo(call.message.chat.id, images[0], caption=message_text, reply_markup=markup,
                                    parse_mode='Markdown')
            except Exception as e:
                self.bot.send_message(call.message.chat.id, message_text, reply_markup=markup, parse_mode='Markdown')
        else:
            self.bot.send_message(call.message.chat.id, message_text, reply_markup=markup, parse_mode='Markdown')

        self.bot.answer_callback_query(call.id)

    def show_settings(self, call):
        chat_id = call.message.chat.id

        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("🔔 Настройки уведомлений", callback_data="settings_notify"),
            InlineKeyboardButton("⏰ Интервал парсинга", callback_data="settings_interval"),
            InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
        )

        self.bot.send_message(chat_id, "⚙️ *Настройки бота*", reply_markup=markup, parse_mode='Markdown')

    def get_recent_objects(self, offset: int = 0, limit: int = 5):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                'SELECT id, title, price, address, desc, images, link FROM apartments ORDER BY id DESC LIMIT ? OFFSET ?',
                (limit, offset))

            objects = []
            for row in cursor.fetchall():
                objects.append({
                    'id': row[0], 'title': row[1] or "Без названия", 'price': row[2] or "Не указана",
                    'address': row[3] or "Не указан", 'desc': row[4] or "Описание отсутствует",
                    'images': row[5], 'link': row[6] or ""
                })

            conn.close()
            return objects
        except Exception:
            # Если базы нет или ошибка - возвращаем пустой список
            return []

    def get_object_by_id(self, object_id: int):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT id, title, price, address, desc, images, link FROM apartments WHERE id = ?',
                           (object_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                return {
                    'id': row[0], 'title': row[1] or "Без названия", 'price': row[2] or "Не указана",
                    'address': row[3] or "Не указан", 'desc': row[4] or "Описание отсутствует",
                    'images': row[5], 'link': row[6] or ""
                }
            return None
        except Exception:
            return None

    def get_total_objects(self):
        """Получить общее количество объектов"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM apartments')
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception:
            # Если базы нет или ошибка - возвращаем 0
            return 0

    def get_parsing_stats(self):
        """Получить детальную статистику парсинга"""
        total_objects = self.get_total_objects()

        # В реальной реализации здесь нужно хранить статистику каждого запуска
        # Пока возвращаем данные на основе текущего состояния БД
        return {
            'total_processed': total_objects,
            'new_items': total_objects,  # Предполагаем, что все объекты новые
            'existing_items': 0,
            'errors': 0,
            'total_in_db': total_objects
        }

    def run(self):
        logger.info("Бот запущен...")
        try:
            self.bot.infinity_polling()
        except Exception as e:
            logger.error(f"Ошибка бота: {e}")


# Точка входа
if __name__ == "__main__":
    try:
        bot = AvitoTelegramBot()
        bot.run()
    except ValueError as e:
        logger.error(f"Ошибка инициализации бота: {e}")
        print(f"❌ Ошибка: {e}")
        print("💡 Убедитесь, что в корне проекта есть файл .env с переменной BOT_TOKEN")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
