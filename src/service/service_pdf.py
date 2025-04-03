import asyncio
import base64
import os
import uuid
from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Any
import tempfile
import qrcode
from PIL import Image
from fpdf import FPDF
from pydantic import BaseModel
from src.logger import app_logger as logger

from src.response import AsyncHttpClient


class OrderItem(BaseModel):
    """Класс для представления данных заказа"""
    order_id: str = ""
    subject_name: str = ""
    file: str = ""
    article: str = ""
    supply_id: str = ""
    partA: str = ""
    partB: str = ""
    photo_link: str = ""
    photo_img: str = ""


class ImageService:
    """Сервис для работы с изображениями"""

    @staticmethod
    def create_temp_dir(path="src/service/temp"):
        """Создает временную директорию, если она не существует"""
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    async def download_and_encode_image(url: str) -> str:
        """
        Скачивает изображение по ссылке url и возвращает его в виде base64-строки.
        Если произошла ошибка, возвращает пустую строку.
        """
        try:
            response = await AsyncHttpClient().get(url)
            img = Image.open(BytesIO(response.content))
            img.verify()
            base64_str = base64.b64encode(response.content).decode('utf-8')
            return base64_str
        except Exception as e:
            logger.info(f"Ошибка при скачивании/кодировании изображения {url}: {e}")
            return ""

    @staticmethod
    def generate_qr_code(qr_data: str) -> str:
        """Генерирует QR-код и возвращает путь к временному файлу"""
        temp_dir = ImageService.create_temp_dir()

        qr = qrcode.QRCode(
            version=1,
            box_size=12,
            border=4
        )
        qr.add_data(qr_data)
        qr.make(fit=True)

        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_filename = f"{temp_dir}/qr_code_{uuid.uuid4()}.png"
        qr_img.save(qr_filename)

        return qr_filename


class PDFService:
    """Сервис для работы с PDF"""

    def __init__(self):
        self.current_dir = os.path.dirname(__file__)
        self.dejavu_regular_path = os.path.join(self.current_dir, "DejaVuSansCondensed.ttf")
        self.dejavu_bold_path = os.path.join(self.current_dir, "DejaVuSans-Bold.ttf")

    def create_sticker_pdf(self, stickers: Dict[str, List[Dict[str, Any]]]) -> BytesIO:
        """Создает PDF с стикерами"""
        pdf_buffer = BytesIO()
        pdf = FPDF(unit="mm", format=(58, 40))
        pdf.set_margins(0, 0, 0)
        pdf.set_auto_page_break(auto=False)

        for key, orders in stickers.items():
            if not isinstance(orders, list):
                continue
            pdf.add_font('DejaVu', '', self.dejavu_regular_path)
            pdf.set_font("DejaVu", size=8)
            pdf.add_page()

            qr_data_left = f"{key}"
            qr_data_right = f"{key}/{len(orders)}"

            image_service = ImageService()
            qr_filename_left = image_service.generate_qr_code(qr_data_left)
            qr_filename_right = image_service.generate_qr_code(qr_data_right)

            pdf.set_font("DejaVu", size=12)
            pdf.cell(58, 7, orders[0]["subject_name"], ln=True, align='C')

            pdf.set_font("DejaVu", size=16)
            pdf.set_x((58 - pdf.get_string_width(f"{key}")) / 2)
            pdf.cell(pdf.get_string_width(f"{key}"), 8, f"{key}", ln=True)

            pdf.image(qr_filename_left, x=1, y=15, w=26, h=26)

            pdf.set_font("DejaVu", size=10)

            pdf.set_xy(25, 27)
            pdf.cell(8, 5, str(len(orders)), align='C')


            pdf.set_font("DejaVu", size=12)
            pdf.set_xy(25, 32)
            pdf.cell(8, 5, "▼", align='C')

            pdf.image(qr_filename_right, x=32, y=15, w=26, h=26)

            os.remove(qr_filename_left)
            os.remove(qr_filename_right)

            self._process_sticker_images(pdf, orders)

        pdf_buffer.write(pdf.output(dest="S"))
        pdf_buffer.seek(0)

        return pdf_buffer

    def _process_sticker_images(self, pdf: FPDF, orders: List[Dict[str, Any]]) -> None:
        """Обрабатывает изображения для стикеров"""
        for index, order in enumerate(orders, 1):
            if "file" in order and order["file"]:
                try:
                    pdf.add_page()

                    img_data = base64.b64decode(order["file"])
                    img = Image.open(BytesIO(img_data))

                    temp_dir = ImageService.create_temp_dir()

                    unique_filename = f"{temp_dir}/temp_image_{uuid.uuid4()}.jpg"
                    img.save(unique_filename, "JPEG")

                    pdf.image(unique_filename, x=0, y=0, w=58, h=40)

                    os.remove(unique_filename)

                except Exception as e:
                    pdf.add_page()
                    pdf.set_font("DejaVu", size=6)
                    pdf.cell(58, 5, f"Image Load Error: {str(e)}", ln=True)

    async def create_table_pdf(self, data_list: Dict[str, List[Dict[str, Any]]]) -> BytesIO:
        """Создает PDF с таблицей данных"""
        await self._load_photos(data_list)

        pdf = FPDF(orientation='L', unit='mm', format='A4')
        pdf.add_font('DejaVu', '', self.dejavu_regular_path)
        pdf.add_font('DejaVu', 'B', self.dejavu_bold_path)
        pdf.set_font("DejaVu", size=8)

        col_headers = [
            "№ Сборки", "Наименование", "Фото", "Артикул Поставщика", "Артикул",
            "№ Поставки", "Кабинет", "Стикер"
        ]
        col_widths = [30, 60, 30, 30, 30, 30, 20, 30]
        row_height = 30

        total = sum(len(orders) for orders in data_list.values())

        first_page = True

        for _, value in data_list.items():
            for order in value:
                if first_page or pdf.get_y() + row_height > pdf.page_break_trigger:
                    pdf.add_page()

                    if first_page:
                        pdf.set_font("DejaVu", "B", size=10)
                        pdf.cell(0, 10, f"Количество товаров: {total}", ln=True, align="L")
                        self._print_table_header(pdf, col_headers, col_widths)
                        first_page = False
                        pdf.set_font("DejaVu", "", 8)

                self._draw_table_row(pdf, order, col_widths, row_height)

        pdf_buffer = BytesIO()
        pdf_buffer.write(pdf.output(dest='S'))
        pdf_buffer.seek(0)
        return pdf_buffer

    async def _load_photos(self, data_list: Dict[str, List[Dict[str, Any]]]) -> None:
        """Загружает фотографии для всех заказов"""
        tasks = []

        for _, value in data_list.items():
            for order in value:
                if 'НЕТ' in order["photo_link"]:
                    order["photo_img"] = order["photo_link"]
                else:
                    tasks.append((order, AsyncHttpClient().get(order["photo_link"])))

        results = await asyncio.gather(*(task[1] for task in tasks))

        for (order, _), photo in zip(tasks, results):
            order["photo_img"] = photo

    def _print_table_header(self, pdf: FPDF, col_headers: List[str], col_widths: List[int]) -> None:
        """Печатает заголовок таблицы"""
        pdf.set_font("DejaVu", size=8)
        for i, header in enumerate(col_headers):
            pdf.cell(col_widths[i], 10, header, border=1, align='C')
        pdf.ln()

    def _draw_table_row(self, pdf: FPDF, order: Dict[str, Any], col_widths: List[int], row_height: int) -> None:
        """Отрисовывает строку таблицы"""
        pdf.set_y(pdf.get_y())

        pdf.cell(col_widths[0], row_height, str(order.get("order_id", "")), border=1, align='C')
        pdf.cell(col_widths[1], row_height, str(order.get("subject_name", "")), border=1, align='C')

        cell_x = pdf.get_x()
        cell_y = pdf.get_y()
        pdf.cell(col_widths[2], row_height, "", border=1)
        self._insert_image_in_cell(pdf, order.get("photo_img", ""), cell_x, cell_y, col_widths[2], row_height)

        pdf.cell(col_widths[3], row_height, str(order.get("article", "")), border=1, align='C')
        pdf.cell(col_widths[4], row_height, str(order.get("nm_id", "")), border=1, align='C')
        pdf.cell(col_widths[5], row_height, str(order.get("supply_id", "")), border=1, align='C')
        pdf.cell(col_widths[6], row_height, str(order.get("account", "")), border=1, align='C')

        x = pdf.get_x()
        y = pdf.get_y()
        pdf.cell(col_widths[7], row_height, "", border=1)

        pdf.set_xy(x, y)
        partA = str(order.get('partA', ''))
        partB = " " + f"{order.get('partB', ''):04d}"

        pdf.set_font("DejaVu", "", 8)
        widthA = pdf.get_string_width(partA)
        pdf.cell(widthA, row_height, partA, border=0, ln=0)

        pdf.set_font("DejaVu", "B", 8)
        pdf.cell(pdf.get_string_width(partB), row_height, partB, border=0, ln=0)
        pdf.set_font("DejaVu", "", 8)

        pdf.ln(row_height)

    def _insert_image_in_cell(self, pdf_obj: FPDF, b64_string: str, cell_x: float, cell_y: float, cell_width: float,
                              cell_height: float) -> None:
        """Вставляет изображение внутрь заданной ячейки таблицы"""
        if not b64_string:
            return

        try:
            img = Image.open(BytesIO(b64_string))
            with tempfile.NamedTemporaryFile(delete=False, suffix=".webp") as temp_file:
                temp_filename = temp_file.name
                img.save(temp_filename, "WEBP")

            img_width = cell_width - 4
            img_height = cell_height - 4
            img_x = cell_x + 2
            img_y = cell_y + 2

            pdf_obj.image(temp_filename, x=img_x, y=img_y, w=img_width, h=img_height)
            os.remove(temp_filename)

        except Exception as e:
            pdf_obj.set_font("DejaVu", "B", size=6)
            text_width = pdf_obj.get_string_width(b64_string)
            text_x = cell_x + (cell_width - text_width) / 2
            text_y = cell_y + (cell_height - 6) / 2
            pdf_obj.set_xy(text_x, text_y)
            pdf_obj.cell(text_width, 6, b64_string, align='C', border=0)
            pdf_obj.set_font("DejaVu", size=8)
            pdf_obj.set_xy(cell_x + cell_width, cell_y)


class DataProcessor:
    """Класс для обработки данных"""

    @staticmethod
    async def fill_base64_in_dict(data: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Для каждого элемента в data (где значение — список словарей)
        проверяет поле 'file' и, если оно пустое, скачивает изображение по 'photo_link'.
        """
        image_service = ImageService()
        tasks = []

        for key, items in data.items():
            if not isinstance(items, list):
                continue

            for item in items:
                if not item.get('file'):
                    link = item.get('photo_link')
                    if link:
                        tasks.append((item, link))

        if tasks:
            download_tasks = [image_service.download_and_encode_image(link) for _, link in tasks]
            results = await asyncio.gather(*download_tasks)

            for (item, _), result in zip(tasks, results):
                item['file'] = result


async def collect_images_sticker_to_pdf(stickers: Dict[str, List[Dict[str, Any]]]) -> BytesIO:
    """Создает PDF со стикерами"""
    pdf_service = PDFService()
    return pdf_service.create_sticker_pdf(stickers)


async def download_and_encode_image(url: str) -> str:
    """Скачивает и кодирует изображение в base64"""
    image_service = ImageService()
    return await image_service.download_and_encode_image(url)


async def fill_base64_in_dict(data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Заполняет base64-строки в словаре"""
    processor = DataProcessor()
    await processor.fill_base64_in_dict(data)


async def create_table_pdf(data_list: Dict[str, List[Dict[str, Any]]]) -> BytesIO:
    """Создает PDF с таблицей"""
    pdf_service = PDFService()
    return await pdf_service.create_table_pdf(data_list)
