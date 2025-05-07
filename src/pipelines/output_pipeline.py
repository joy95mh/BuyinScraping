from src.utils.file_handler import update_input_file
import logging
from datetime import datetime
import os
import shutil
import time
import json

# Configure module-level logger with a simplified name
logger = logging.getLogger("pipeline")

class OutputPipeline:
    def __init__(self):
        self.items = []
        # Create a directory for stats if it doesn't exist
        stats_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "stats")
        if not os.path.exists(stats_dir):
            os.makedirs(stats_dir)
        self.stats_dir = stats_dir

    def process_item(self, item, spider):
        # Store the item for bulk processing
        self.items.append(item)
        # Increment the processed count in the spider
        if hasattr(spider, 'increment_processed_count'):
            spider.increment_processed_count()
        return item

    def close_spider(self, spider):
        if not self.items:
            logger.info("No items were scraped")
            return

        # Try to get the input file path from the spider first (for parallel mode),
        # then from settings (for standard mode)
        input_file = getattr(spider, 'input_file', None) or spider.crawler.settings.get('INPUT_FILE')
        
        if not input_file:
            logger.error("No input_file found in spider or settings")
            return
        
        # Calculate and log spider stats
        stats = None
        if hasattr(spider, 'finish_stats'):
            stats = spider.finish_stats()
            
            # Save stats to a JSON file
            if stats:
                self._save_spider_stats(stats)
        
        # First, update the original file to ensure data is saved
        update_count = update_input_file(input_file, self.items)
        
        if update_count > 0:
            # If updates were made, create a copy with the date
            output_file = self._rename_file_with_date(input_file)
            logger.info(f"Updated original file and created output copy: {output_file}")
            
            # Log performance information
            if stats:
                logger.info(f"Spider {spider.name} performance summary:")
                logger.info(f"- Time taken: {stats['total_duration_formatted']}")
                logger.info(f"- Items processed: {stats['processed_items']}")
                logger.info(f"- Items/minute: {stats['items_per_minute']:.2f}")
        else:
            logger.info("No rows were updated, skipping output copy creation")

    def _save_spider_stats(self, stats):
        """Save spider statistics to a JSON file, overwriting any existing file"""
        try:
            # Create a filename without datetime to allow overwriting old stats
            spider_name = stats.get("spider_name", "unknown")
            filename = f"{spider_name}_stats.json"
            
            # Full path to the stats file
            stats_file = os.path.join(self.stats_dir, filename)
            
            # Add a timestamp to the stats
            stats["timestamp"] = datetime.now().isoformat()
            
            # Save to file (overwriting any existing file)
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2)
                
            logger.info(f"Saved spider stats to {stats_file}")
        except Exception as e:
            logger.error(f"Error saving spider stats: {str(e)}")

    def _rename_file_with_date(self, input_file):
        if not input_file or not os.path.exists(input_file):
            return input_file

        try:
            directory = os.path.dirname(input_file)
            basename = os.path.basename(input_file)
            filename, ext = os.path.splitext(basename)

            current_date = datetime.now().strftime("%Y%m%d")
            # Avoid brackets
            new_filename = f"Output_{current_date}_{filename}{ext}"
            new_path = os.path.join(directory, new_filename)

            # Ensure file is not being written to
            time.sleep(1)
            shutil.copy2(input_file, new_path)

            logger.info(f"Created output copy: {new_filename}")
            return new_path
        except Exception as e:
            logger.error(f"Error creating file copy: {str(e)}")
            return input_file
