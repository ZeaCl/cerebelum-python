"""Entry point for python -m cerebelum.worker"""
from . import main_worker
import asyncio
asyncio.run(main_worker())
