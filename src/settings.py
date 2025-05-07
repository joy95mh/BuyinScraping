import random

BOT_NAME = "buyin_scraping"
SPIDER_MODULES = ["src.spiders"]
NEWSPIDER_MODULE = "src.spiders"
RETRY_TIMES = 3
DOWNLOAD_TIMEOUT = 30

# Configure item pipelines
ITEM_PIPELINES = {
    "src.pipelines.output_pipeline.OutputPipeline": 300,
}

CONCURRENT_REQUESTS = 16
DOWNLOAD_DELAY = 0.5

