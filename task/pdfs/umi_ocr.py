import os
import yaml
from pathlib import Path
import aiohttp

class UmiOcr:
    def __init__(self):
        # 读取配置文件
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yml')
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
            
        self.url = self.config['pdfocr']['url']
        self.client = aiohttp.ClientSession()
        self.public_path = Path(__file__).parent.parent / 'public'
        
    async def upload(self, file_path: str) -> dict:
        """上传PDF文件"""
        url = f"{self.url}/api/doc/upload"
        file_path = self.public_path / file_path
        
        try:
            with open(file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('file', f)
                
                async with self.client.post(url, data=data) as resp:
                    return await resp.json()
        except Exception as e:
            return {'code': 500, 'data': str(e)}
            
    async def result(self, task_id: str) -> dict:
        """查询任务状态"""
        url = f"{self.url}/api/doc/result"
        payload = {
            'id': task_id,
            'is_data': False,
            'is_unread': True,
            'format': 'dict'
        }
        
        try:
            async with self.client.post(url, json=payload) as resp:
                return await resp.json()
        except Exception as e:
            return {'code': 500, 'data': str(e)}
            
    async def download(self, task_id: str, file_types: list) -> dict:
        """获取下载链接"""
        url = f"{self.url}/api/doc/download"
        payload = {
            'id': task_id,
            'file_types': file_types
        }
        
        try:
            async with self.client.post(url, json=payload) as resp:
                return await resp.json()
        except Exception as e:
            return {'code': 500, 'data': str(e)}
            
    async def save_file(self, url: str, save_path: str) -> bool:
        """保存文件到本地"""
        save_path = self.public_path / save_path
        try:
            async with self.client.get(url) as resp:
                with open(save_path, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
                return True
        except Exception as e:
            print(f"Save file error: {str(e)}")
            return False
            
    async def get_file_content(self, url: str) -> str:
        """获取文件内容"""
        try:
            async with self.client.get(url) as resp:
                return await resp.text()
        except Exception as e:
            print(f"Get file content error: {str(e)}")
            return ""
            
    async def close(self):
        """关闭HTTP客户端"""
        await self.client.close()
