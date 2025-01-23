import os
import yaml
import asyncio
import aiomysql
import json
import logging
from .umi_ocr import UmiOcr

logger = logging.getLogger('PDFProcessor')

class PDFProcessor:
    def __init__(self):
        # 读取配置文件
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yml')
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 数据库配置
        self.db_config = self.config['database'].copy()
        if 'database' in self.db_config:
            self.db_config['db'] = self.db_config.pop('database')
        
        self.pool = None  # 数据库连接池
        self.batch_size = self.config.get('pdf', {}).get('batch_size', 10)  # 从配置文件读取批量处理数量
        self.task_table = 'ww_pdf_task'
        self.task_page = 'ww_document_pages'
        self.semaphore = asyncio.Semaphore(self.config.get('pdf', {}).get('max_concurrent', 3))  # 并发控制
        
    async def connect_db(self):
        """创建数据库连接池"""
        try:
            self.pool = await aiomysql.create_pool(
                minsize=1,
                maxsize=10,
                **self.db_config
            )
            logger.info("数据库连接池创建成功")
        except Exception as e:
            logger.error(f"数据库连接池创建失败: {str(e)}")
            raise
        
    async def get_pending_tasks(self) -> list:
        """获取待处理任务"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(
                        f"SELECT * FROM {self.task_table} WHERE status = 'pending' LIMIT {self.batch_size}"
                    )
                    return await cursor.fetchall()
        except Exception as e:
            logger.error(f"获取待处理任务失败: {str(e)}")
            return []
                
    async def get_processing_task(self, task_id: int) -> dict:
        """获取处理中任务"""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    f"SELECT * FROM {self.task_table} WHERE id = %s AND status = 'processing'",
                    (task_id,)
                )
                return await cursor.fetchone()
                
    async def get_downloading_task(self, task_id: int) -> dict:
        """获取下载中任务"""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    f"SELECT * FROM {self.task_table} WHERE id = %s AND status = 'downloading'",
                    (task_id,)
                )
                return await cursor.fetchone()
                
    async def _process_pending_task(self, task):
        """处理单个待处理任务"""
        async with self.semaphore:  # 使用信号量控制并发
            try:
                logger.info(f"开始处理待处理任务 ID: {task['id']}")
                await self.upload(task['id'])
                await self.update_status(task['id'], 'processing')
            except Exception as e:
                logger.error(f"处理待处理任务失败 ID {task['id']}: {str(e)}")
                await self.update_status(task['id'], 'error')
                raise
            
    async def _process_processing_task(self, task) -> bool:
        """处理单个处理中任务"""
        async with self.semaphore:  # 使用信号量控制并发
            try:
                logger.info(f"开始处理进行中任务 ID: {task['id']}")
                result = await self.result(task['id'])
                if result:
                    await self.update_status(task['id'], 'downloading')
                return result
            except Exception as e:
                logger.error(f"处理进行中任务失败 ID {task['id']}: {str(e)}")
                await self.update_status(task['id'], 'error')
                return False
            
    async def _process_downloading_task(self, task):
        """处理单个下载任务"""
        async with self.semaphore:  # 使用信号量控制并发
            try:
                logger.info(f"开始处理下载任务 ID: {task['id']}")
                await self.download(task['id'])
                await self.update_status(task['id'], 'completed')
                logger.info(f"任务处理完成 ID: {task['id']}")
            except Exception as e:
                logger.error(f"处理下载任务失败 ID {task['id']}: {str(e)}")
                await self.update_status(task['id'], 'error')
                raise
            
    async def update_status(self, task_id: int, status: str):
        """更新任务状态和最后更新时间"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"UPDATE {self.task_table} SET status = %s, updated_at = NOW() WHERE id = %s",
                    (status, task_id)
                )
                await conn.commit()
                
    async def reset_stale_tasks(self, timeout_minutes: int = 30):
        """重置超时的处理中任务"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"UPDATE {self.task_table} SET status = 'pending' "
                    f"WHERE status = 'processing' "
                    f"AND (updated_at IS NULL OR updated_at < DATE_SUB(NOW(), INTERVAL %s MINUTE))",
                    (timeout_minutes,)
                )
                await conn.commit()

    async def upload(self, task_id: int):
        """上传PDF文件"""
        file = await self.get_pdf_file(task_id)
        if not file:
            raise Exception("文件不存在")
            
        if not file['task_id']:
            result = await UmiOcr().upload(self.config['app']['resource_path'] + file['origin_path'])
            if result['code'] != 100:
                await self.update_pdf_file_task_error(file['id'], result['data'])
                raise Exception(result['data'])
                
            await self.update_pdf_file_task_id(file['id'], result['data'])

    async def result(self, task_id: int) -> bool:
        """获取任务结果"""
        file = await self.get_pdf_file(task_id)
        if not file:
            raise Exception("文件不存在")
            
        if file['task_id'] and file['task_status'] != 'success':
            result = await UmiOcr().result(file['task_id'])
            if result['code'] != 100:
                await self.update_pdf_file_task_error(file['id'], result['data'])
                raise Exception(result['data'])
                
            await self.update_pdf_file_task_status(
                file['id'],
                result['state'],
                f"{result['processed_count']}/{result['pages_count']}",
                json.dumps(result)
            )
            return result['state'] == 'success'
        return False

    async def download(self, task_id: int):
        """下载处理结果"""
        file = await self.get_pdf_file(task_id)
        if not file:
            logger.error(f"下载失败：文件不存在 ID {task_id}")
            raise Exception("文件不存在")
            
        if file['task_id'] and file['task_status'] == 'success':
            try:
                await asyncio.gather(
                    self.download_pdf(file),
                    self.download_txt(file)
                )
                logger.info(f"文件下载完成 ID {task_id}")
            except Exception as e:
                logger.error(f"文件下载失败 ID {task_id}: {str(e)}")
                raise

    async def get_pdf_file(self, task_id: int) -> dict:
        """获取PDF文件信息"""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    f"SELECT * FROM ww_pdf_file WHERE id = %s",
                    (task_id,)
                )
                return await cursor.fetchone()

    async def update_pdf_file_task_id(self, file_id: int, task_id: str):
        """更新PDF文件任务ID"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "UPDATE ww_pdf_file SET task_id = %s WHERE id = %s",
                    (task_id, file_id)
                )
                await conn.commit()

    async def update_pdf_file_task_error(self, file_id: int, error: str):
        """更新PDF文件错误信息"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "UPDATE ww_pdf_file SET task_error = %s WHERE id = %s",
                    (error, file_id)
                )
                await conn.commit()

    async def update_pdf_file_task_status(self, file_id: int, status: str, process: str, result: str):
        """更新PDF文件任务状态"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "UPDATE ww_pdf_file SET task_status = %s, task_process = %s, task_result = %s WHERE id = %s",
                    (status, process, result, file_id)
                )
                await conn.commit()

    async def download_pdf(self, file: dict):
        """下载PDF文件"""
        if file['target_path']:
            return
            
        result = await UmiOcr().download(file['task_id'], ['pdfLayered'])
        if result['code'] == 100:
            file_path = f"{self.config['app']['resource_path']}{file['origin_path']}.layered.pdf"
            if await UmiOcr().save_file(result['data'], file_path):
                await self.update_pdf_file_target_path(file, file_path)
            else:
                await self.update_pdf_file_task_error(file['id'], '下载文件失败')
                raise Exception('下载文件失败')
        else:
            await self.update_pdf_file_task_error(file['id'], result['data'])
            raise Exception(result['data'])

    async def download_txt(self, file: dict):
        """下载TXT文件"""
        if file['target_txt']:
            return
            
        result = await UmiOcr().download(file['task_id'], ['txt'])
        if result['code'] == 100:
            txt = await UmiOcr().get_file_content(result['data'])
            if txt:
                await self.update_pdf_file_target_txt(file['id'], txt)
            else:
                await self.update_pdf_file_task_error(file['id'], '下载TXT文件失败')
                raise Exception('下载TXT文件失败')
        else:
            await self.update_pdf_file_task_error(file['id'], result['data'])
            raise Exception(result['data'])

    async def update_pdf_file_target_path(self, file: dict, target_path: str):
        """更新PDF文件目标路径"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "UPDATE ww_pdf_file SET target_path = %s WHERE id = %s",
                    (target_path, file['id'])
                )
                await conn.commit()

    async def update_pdf_file_target_txt(self, file_id: int, target_txt: str):
        """更新PDF文件目标TXT"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "UPDATE ww_pdf_file SET target_txt = %s WHERE id = %s",
                    (target_txt, file_id)
                )
                await conn.commit()

    async def close(self):
        """关闭所有资源"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
        
        # 关闭UmiOcr的HTTP客户端
        await UmiOcr().close()
