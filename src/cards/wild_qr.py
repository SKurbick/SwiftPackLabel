import io
import platform
import textwrap

import pymupdf
import qrcode
from PIL import Image, ImageDraw, ImageFont
from qrcode.constants import ERROR_CORRECT_L


class WildQrPdfGenerator:
    _label_width = 354
    _label_height = 236
    _label_width_pt = 30 * 72 / 25.4
    _label_height_pt = 20 * 72 / 25.4
    _label_qr_size = 120

    def create_document(self, wild: str, name: str, count: int) -> io.BytesIO:
        label = self.create_label(wild, name)
        label_bytes = label.getvalue()
        document = pymupdf.open()
        for _ in range(count):
            page = document.new_page(
                width=self._label_width_pt,
                height=self._label_height_pt,
            )
            page.insert_image(page.rect, stream=label_bytes)
        result = io.BytesIO(document.tobytes(deflate=True, garbage=3))
        document.close()
        result.seek(0)
        return result

    def create_label(self, wild: str, name: str) -> io.BytesIO:
        image = Image.new(
            "RGB",
            (self._label_width, self._label_height),
            color="white",
        )
        qr_code = Image.open(self._create_qr_code(wild)).crop()
        qr_code = qr_code.resize(
            (self._label_qr_size, self._label_qr_size),
            Image.Resampling.LANCZOS,
        )
        image.paste(qr_code, ((self._label_width - self._label_qr_size) // 2, 10))

        draw = ImageDraw.Draw(image)
        if platform.system() == "Darwin":
            font_path = "/System/Library/Fonts/Helvetica.ttc"
        else:
            font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        wild_font = ImageFont.truetype(font_path, size=27)
        name_font = ImageFont.truetype(font_path, size=17)

        lines = [(wild, wild_font)]
        lines += [
            (line, name_font)
            for line in textwrap.wrap(name, width=28, max_lines=2, placeholder="…")
        ]
        y = 10 + self._label_qr_size + 8
        for line, font in lines:
            bbox = draw.textbbox((0, 0), line, font=font)
            draw.text(
                ((self._label_width - (bbox[2] - bbox[0])) // 2, y),
                line,
                fill="black",
                font=font,
            )
            y += bbox[3] + 4

        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        buffer.seek(0)
        return buffer

    @staticmethod
    def _create_qr_code(data: str) -> io.BytesIO:
        qr = qrcode.QRCode(
            version=1,
            error_correction=ERROR_CORRECT_L,
            box_size=12,
            border=0,
        )
        qr.add_data(data)
        qr.make(fit=True)
        buffer = io.BytesIO()
        qr.make_image(fill_color="black", back_color="white").save(
            buffer,
            format="PNG",
        )
        buffer.seek(0)
        return buffer
