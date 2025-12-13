"""
Entry point wrapper for 1Office Daily Sync.
Can be triggered by Cloud Scheduler or manually.
"""
from src.features.one_office.pipeline import OneOfficePipeline
from src.shared.logging import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Triggering 1Office Daily Sync...")
    pipeline = OneOfficePipeline()
    result = pipeline.run_daily_snapshot()
    logger.info(f"Sync Result: {result}")

if __name__ == "__main__":
    main()
