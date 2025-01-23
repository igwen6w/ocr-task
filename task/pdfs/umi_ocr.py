import os
import yaml
import logging
from pathlib import Path
import aiohttp

logger = logging.getLogger('UmiOcr')

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
        
        if not file_path.exists():
            logger.error(f"上传失败：文件不存在 {file_path}")
            return {'code': 404, 'data': '文件不存在'}
        
        try:
            with open(file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('file', f)
                
                async with self.client.post(url, data=data) as resp:
                    result = await resp.json()
                    logger.info(f"文件上传成功: {file_path}")
                    return result
        except aiohttp.ClientError as e:
            logger.error(f"上传请求失败: {str(e)}")
            return {'code': 500, 'data': f'上传请求失败: {str(e)}'}
        except Exception as e:
            logger.error(f"文件上传失败: {str(e)}")
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
                result = await resp.json()
                logger.info(f"获取任务状态成功: {task_id}")
                return result
        except aiohttp.ClientError as e:
            logger.error(f"获取任务状态请求失败: {str(e)}")
            return {'code': 500, 'data': f'获取任务状态请求失败: {str(e)}'}
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}")
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
                result = await resp.json()
                logger.info(f"获取下载链接成功: {task_id}")
                return result
        except aiohttp.ClientError as e:
            logger.error(f"获取下载链接请求失败: {str(e)}")
            return {'code': 500, 'data': f'获取下载链接请求失败: {str(e)}'}
        except Exception as e:
            logger.error(f"获取下载链接失败: {str(e)}")
            return {'code': 500, 'data': str(e)}
            
    async def save_file(self, url: str, save_path: str) -> bool:
        """保存文件到本地"""
        save_path = self.public_path / save_path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            async with self.client.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"下载文件失败，状态码: {resp.status}")
                    return False
                    
                with open(save_path, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(8192)  # 增加缓冲区大小
                        if not chunk:
                            break
                        f.write(chunk)
                logger.info(f"文件保存成功: {save_path}")
                return True
        except aiohttp.ClientError as e:
            logger.error(f"下载文件请求失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"保存文件失败: {str(e)}")
            return False
            
    async def get_file_content(self, url: str) -> str:
        """获取文件内容"""
        try:
            async with self.client.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"获取文件内容失败，状态码: {resp.status}")
                    return ""
                    
                content = await resp.text()
                logger.info("文件内容获取成功")
                return content
        except aiohttp.ClientError as e:
            logger.error(f"获取文件内容请求失败: {str(e)}")
            return ""
        except Exception as e:
            logger.error(f"获取文件内容失败: {str(e)}")
            return ""
            
    async def close(self):
        """关闭HTTP客户端"""
        if not self.client.closed:
            await self.client.close()
            logger.info("HTTP客户端已关闭")
