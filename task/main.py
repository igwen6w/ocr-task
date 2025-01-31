import asyncio
from pages.main import OCRProcessor
from pdfs.main import PDFTaskRunner
import yaml
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class TaskManager:
    def __init__(self):
        # 从环境变量读取配置
        self.config = {
            'database': {
                'host': os.getenv('DB_HOST'),
                'port': os.getenv('DB_PORT', '3306'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'database': os.getenv('DB_NAME')
            },
            'ocr': {
                'url': os.getenv('OCR_URL'),
                'options': {
                    'language': 'models/config_chinese.txt',
                    'cls': True,
                    'limit_side_len': 999999,
                    'parser': 'none',
                    'format': 'text'
                }
            },
            'pdfocr': {
                'url': os.getenv('PDFOCR_URL')
            }
        }

    async def process_pages(self):
        """处理页面OCR任务"""
        processor = OCRProcessor()
        while True:
            try:
                await processor.run_async()
            except Exception as e:
                print(f"页面处理发生错误: {str(e)}")
                await asyncio.sleep(5)

    async def process_pdfs(self):
        """处理PDF任务"""
        from pdfs.main import PDFTaskRunner
        runner = PDFTaskRunner()
        while True:
            try:
                await runner.run()
            except Exception as e:
                print(f"PDF处理发生错误: {str(e)}")
                await asyncio.sleep(5)

    async def run(self):
        """运行所有任务"""
        tasks = [
            self.process_pages(),
            self.process_pdfs()
        ]
        await asyncio.gather(*tasks)

def main():
    manager = TaskManager()
    asyncio.run(manager.run())

if __name__ == "__main__":
    main()
