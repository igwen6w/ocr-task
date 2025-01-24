import asyncio
import signal
import logging
import os
import yaml
import aiomysql
from .pdf_processor import PDFProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('PDFTaskRunner')

async def shutdown(signal, loop):
    logger.info(f"收到退出信号 {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f"正在取消 {len(tasks)} 个未完成的任务")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

class PDFTaskRunner:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'config'):
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yml')
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            self.db_config = self.config['database'].copy()
            if 'database' in self.db_config:
                self.db_config['db'] = self.db_config.pop('database')

    async def _process_task(self, processor, task):
        logger.info(f"开始处理任务 ID: {task['id']}")
        await processor._process_pending_task(task)
        
        retry_count = 0
        while retry_count < 3:
            try:
                processing_task = await processor.get_processing_task(task['id'])
                if not processing_task:
                    break
                    
                if await processor._process_processing_task(processing_task):
                    downloading_task = await processor.get_downloading_task(task['id'])
                    if downloading_task:
                        await processor._process_downloading_task(downloading_task)
                        logger.info(f"任务 {task['id']} 处理完成")
                    return True
                
                await asyncio.sleep(5)
            except Exception as e:
                retry_count += 1
                logger.warning(f"任务 {task['id']} 处理出错 ({retry_count}/3): {str(e)}")
                if retry_count >= 3:
                    logger.error(f"任务 {task['id']} 处理失败: {str(e)}")
                    return False
        return False

    async def run(self):
        while True:
            try:
                # 创建连接池和处理器
                pool = await aiomysql.create_pool(**self.db_config)
                processor = PDFProcessor(pool)
                await processor.reset_stale_tasks()
                
                # 获取待处理任务
                pending_tasks = await processor.get_pending_tasks()
                
                if not pending_tasks:
                    logger.info("没有待处理的任务")
                else:
                    # 处理任务
                    await self._process_task(processor, pending_tasks[0])
                    
            except asyncio.CancelledError:
                logger.info("正在优雅关闭...")
                break
            except Exception as e:
                logger.error(f"运行时错误: {str(e)}")
            finally:
                if 'processor' in locals():
                    await processor.close()
                if 'pool' in locals():
                    pool.close()
                    await pool.wait_closed()
                await asyncio.sleep(60)

if __name__ == "__main__":
    runner = PDFTaskRunner()
    loop = asyncio.get_event_loop()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s, loop)))
    
    try:
        loop.run_until_complete(runner.run())
    finally:
        loop.close()