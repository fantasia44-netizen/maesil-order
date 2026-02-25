"""
reports — reportlab 출력 템플릿 패키지.
DB 접근 금지. 공통 출력은 pdf_common.py로.
"""
try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                     Paragraph, Spacer, PageBreak, KeepTogether,
                                     BaseDocTemplate, Frame, PageTemplate,
                                     NextPageTemplate, FrameBreak)
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False
