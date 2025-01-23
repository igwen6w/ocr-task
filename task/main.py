import asyncio
from pages.main import OCRProcessor
from pdfs.main import PDFTaskRunner
import yaml
import os

class TaskManager:
    def __init__(self):
        # 读取配置文件
        config_path = os.path.join(os.path.dirname(__file__), 'config.yml')
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

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
