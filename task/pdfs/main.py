import asyncio
import signal
from .pdf_processor import PDFProcessor

async def shutdown(signal, loop):
    """清理任务并关闭"""
    print(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    print(f"Cancelling {len(tasks)} outstanding tasks")
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
            await self.processor.connect_db()
            
            print("PDF processor started.")
            
            try:
                while True:
                    try:
                        await asyncio.gather(
                            self.processor.process_pending_tasks(),
                            self.processor.process_processing_tasks(),
                            self.processor.process_downloading_tasks()
                        )
                        await asyncio.sleep(30)  # 每30秒检查一次
                    except asyncio.CancelledError:
                        print("Shutting down gracefully...")
                        break
                    except Exception as e:
                        print(f"Main loop error: {str(e)}")
                        await asyncio.sleep(60)  # 出错后等待10秒重试
            finally:
                await self.processor.close()

if __name__ == "__main__":
    runner = PDFTaskRunner()
    runner.run()
