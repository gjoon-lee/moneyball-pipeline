import os
import logging
import datetime
import pandas as pd
import pybaseball as pyb

class MoneyballDataCollector:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        self.raw_path = os.path.join(base_path, "raw")
        self.setup_logging()
        self.ensure_directories()

    def setup_logging(self):    #Setting up Logging system
        logging.basicConfig(
            level = logging.INFO,
            format = '%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data_pipeline.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def ensure_directories(self):   #Creates folder if it doesn't exist
        for path in [self.raw_path]:
            os.makedirs(path, exist_ok=True)

    def collect_season_data(self, year):
        try:
            self.logger.info(f"Collecting data for {year}")

            # Collecting batting data for different years
            batting_data = pyb.batting_stats(year)
            batting_file = os.path.join(self.raw_path, f"batting_{year}.csv")
            batting_data.to_csv(batting_file, index=False)

            return True
        
        except Exception as e:
            self.logger.error(f"Error collecting data for {year}: {str(e)}")
            return False

if __name__ == "__main__":
    collector = MoneyballDataCollector()

    years = [2020,2021,2022]
    for year in years:
        success = collector.collect_season_data(year)
        if success:
            print(f"✅ Successfully collected {year} data")
        else:
            print(f"❌ Failed to collect {year} data")