import logging
from openpyxl import workbook
from openpyxl.styles import alignment,Font,Border,PatternFill,Side
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image

logging.basicConfig(filemode='w',filename='logs/redbook1.log',encoding='utf-8',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('logging started')

