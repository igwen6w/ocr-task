import asyncio
import signal
import logging
from .pdf_processor import PDFProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('PDFTaskRunner')

async def shutdown(signal, loop):
    """清理任务并关闭"""
    logger.info(f"收到退出信号 {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f"正在取消 {len(tasks)} 个未完成的任务")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

class PDFTaskRunner:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.processor = PDFProcessor()
            cls._instance.loop = None
        return cls._instance

    async def run(self):
        """运行PDF处理任务"""
        async with self._lock:
            try:
                await self.processor.connect_db()
                # 重置超时任务
                await self.processor.reset_stale_tasks()
                logger.info("PDF处理器已启动")
                
                while True:
                    try:
                        # 处理待处理任务
                        pending_tasks = await self.processor.get_pending_tasks()
                        if pending_tasks:
                            task = pending_tasks[0]  # 只取一个任务处理
                            logger.info(f"开始处理任务 ID: {task['id']}")
                            await self.processor._process_pending_task(task)
                            
                            # 监控任务状态直到完成
                            retry_count = 0
                            max_retries = 3
                            while True:
                                processing_task = await self.processor.get_processing_task(task['id'])
                                if not processing_task:
                                    logger.warning(f"任务 {task['id']} 不在处理中状态")
                                    break
                                    
                                try:
                                    result = await self.processor._process_processing_task(processing_task)
                                    if result:
                                        # 任务处理完成，下载结果
                                        downloading_task = await self.processor.get_downloading_task(task['id'])
                                        if downloading_task:
                                            await self.processor._process_downloading_task(downloading_task)
                                            logger.info(f"任务 {task['id']} 处理完成")
                                        break
                                    retry_count = 0  # 重置重试计数
                                except Exception as e:
                                    retry_count += 1
                                    if retry_count >= max_retries:
                                        logger.error(f"任务 {task['id']} 处理失败，已达到最大重试次数: {str(e)}")
                                        break
                                    logger.warning(f"任务 {task['id']} 处理出错，将重试: {str(e)}")
                                    
                                await asyncio.sleep(5)  # 每5秒检查一次任务状态
                        else:
                            logger.info("没有待处理的任务")
                            await asyncio.sleep(30)  # 无任务时等待30秒
                            
                    except asyncio.CancelledError:
                        logger.info("正在优雅关闭...")
                        break
                    except Exception as e:
                        logger.error(f"主循环错误: {str(e)}")
                        await asyncio.sleep(60)  # 出错后等待60秒重试
            except Exception as e:
                logger.error(f"PDF处理器启动失败: {str(e)}")
            finally:
                logger.info("正在关闭PDF处理器...")
                await self.processor.close()

if __name__ == "__main__":
    runner = PDFTaskRunner()
    loop = asyncio.get_event_loop()
    
    # 注册信号处理
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s, loop)))
    
    try:
        loop.run_until_complete(runner.run())
    finally:
        loop.close()
