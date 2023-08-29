import logging
import os.path
from typing import Callable

from ironsource_report.utils.file_utils import get_filepath_dir, read_csv_files, clear_dir
from ironsource_report.multiquerier.executors.batch_work_executor import BatchWorkExecutor
from ironsource_report.multiquerier.base_job import BaseJob
from ironsource_report import CSVReport


class ExportReportJob(BaseJob):
    def __init__(
        self,
        batch_size,
        max_workers,
        bulk_params: list[dict],
        item_exporter: CSVReport,
        output_dir: str,
        output_file: str,
        gen_filename: Callable[[dict], str] = lambda params: 'default.csv',
        mute_log: bool = False
    ):
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter
        self.bulk_params = bulk_params
        self.output_dir = output_dir
        self.output_file = output_file
        self.gen_filename = gen_filename
        os.makedirs(self.output_dir, exist_ok=True)
        self.batch_work_executor.logger.setLevel('ERROR' if mute_log else 'INFO')
        self.batch_work_executor.progress_logger.logger.setLevel('ERROR' if mute_log else 'INFO')
        logging.getLogger('file_utils').setLevel('ERROR')

    def _start(self):
        pass

    def _export(self):
        self.batch_work_executor.execute(
            self.bulk_params,
            self._export_batch,
            total_items=len(self.bulk_params)
        )

    def _export_batch(self, batch_params):
        for params in batch_params:
            report_df = self.item_exporter.get_report(params=params)
            filename = self.gen_filename(params)
            if not report_df.empty:
                report_df.to_csv(f'{self.output_dir}{filename}', index=False)

    def _end(self):
        self.batch_work_executor.shutdown()
        self._reduce()

    def _reduce(self):
        filepaths = get_filepath_dir(self.output_dir)
        df = read_csv_files(filepaths)
        clear_dir(self.output_dir)
        df.to_csv(f'{self.output_dir}/{self.output_file}', index=False)
