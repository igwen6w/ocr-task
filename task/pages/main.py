import asyncio
import base64
import os
import yaml
from typing import List, Dict

# 在文件开头添加
import aiohttp
import aiomysql

class OCRProcessor:
    def __init__(self):
        # 读取配置文件
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yml')
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 修改数据库配置，将 database 改为 db
        self.db_config = self.config['database'].copy()
        if 'database' in self.db_config:
            self.db_config['db'] = self.db_config.pop('database')
        self.ocr_url = self.config['ocr']['url']
        self.headers = {
            'Content-Type': 'application/json'
        }

    async def get_unprocessed_pages_async(self) -> List[Dict]:
        """异步获取未处理的页面记录"""
        pool = await aiomysql.create_pool(**self.db_config)
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT id, image_path 
                    FROM ww_document_pages 
                    WHERE content IS NULL 
                    AND deleted_at IS NULL
                """)
                result = await cursor.fetchall()
        pool.close()
        await pool.wait_closed()
        return result

    async def process_image_async(self, image_path: str) -> str:
        """异步处理单个图片的OCR识别"""
        try:
            # 读取图片文件并转换为base64
            with open(image_path, 'rb') as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')

            payload = {
                "base64": base64_image,
                "options": {
                    "ocr.language": self.config['ocr']['options']['language'],
                    "ocr.cls": self.config['ocr']['options']['cls'],
                    "ocr.limit_side_len": self.config['ocr']['options']['limit_side_len'],
                    "tbpu.parser": self.config['ocr']['options']['parser'],
                    "data.format": self.config['ocr']['options']['format']
                }
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(self.ocr_url, headers=self.headers, json=payload) as response:
                    if response.status == 200:
                        res = await response.json()
                        return res.get('data')
                    else:
                        raise Exception(f"OCR请求失败: {response.status}")
                
        except Exception as e:
            print(f"处理图片失败: {str(e)}")
            return None

    async def update_page_content_async(self, page_id: int, content: str):
        """异步更新页面内容"""
        pool = await aiomysql.create_pool(**self.db_config)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    UPDATE ww_document_pages 
                    SET content = %s 
                    WHERE id = %s
                """, (content, page_id))
                await conn.commit()
        pool.close()
        await pool.wait_closed()

    async def run_async(self):
        """异步运行任务处理器"""
        pages = await self.get_unprocessed_pages_async()
        
        if not pages:
            print(f"没有需要处理的页面，等待{self.config['task']['wait_time']}秒...")
            await asyncio.sleep(self.config['task']['wait_time'])
            return

        for page in pages:
            print(f"处理页面 ID: {page['id']}")
            content = await self.process_image_async(self.config['app']['resource_path'] + page['image_path'])
            
            if content:
                await self.update_page_content_async(page['id'], content)
                print(f"页面 {page['id']} 处理完成")
            
            await asyncio.sleep(self.config['task']['request_interval'])

if __name__ == "__main__":
    processor = OCRProcessor()
    processor.run()