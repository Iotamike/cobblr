
from cobblr.cobblr_debug import db_print


class CobblrRoutine:
    """
    Do not use or _super this class.
    It is left here as a template for what needs to go into a routine that can re-run itself.
    Inherited async functions aren't recognised as valid coroutines in:
            asyncio.create_task(coroutine)
            queue.put(coroutine)
    Hence _super'ing this class won't work to create a self re-running task as self.method won't be
    recognised as a valid coroutine.
    """
    def __init__(self, queue=None, method=None, ongoing=True):
        self.queue = queue
        self.method = method
        self.ongoing = ongoing

    # starts the routine by adding the run_method to the queue
    async def start(self):
        await self.queue.put(self.run_method())

    # ensures the routine continues to run by re-adding itself to the queue
    async def run_method(self):
        await self.method()
        if self.ongoing:
            await self.queue.put(self.run_method)

    def end(self):
        if self.ongoing:
            self.ongoing = False
        else:
            db_print("task is already ending")