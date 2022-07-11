import csv
import datetime as dt
from collections import Counter
from pathlib import Path

from scrapy.exceptions import DropItem

BASE_DIR = Path(__file__).parent.parent
DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S'


class PepParsePipeline:
    cnt = Counter([])

    def open_spider(self, spider):
        pass

    def process_item(self, item, spider):
        if item.get('status'):
            self.cnt.update([item['status']])
            return item
        else:
            raise DropItem(f"PEP {item['number']}: статус не найден.")

    def close_spider(self, spider):
        peps_per_status = list(self.cnt.items())
        peps_per_status.sort(key=lambda status: status[0])
        total = sum(self.cnt.values())

        header, footer = [('Status', 'Amount')], [('Total', total)]
        results = header + peps_per_status + footer

        results_dir = BASE_DIR / 'results'
        results_dir.mkdir(exist_ok=True)

        now = dt.datetime.now()
        now_formatted = now.strftime(DATETIME_FORMAT)
        file_name = f'status_summary_{now_formatted}.csv'
        file_path = results_dir / file_name
        with open(file_path, 'w', encoding='utf-8') as f:
            writer = csv.writer(f, dialect='unix')
            writer.writerows(results)
