# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from asyncio.log import logger
import json

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_get_log_json_rpc
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.mappers.receipt_mapper import EthReceiptMapper
from ethereumetl.utils import rpc_response_batch_to_results


# Exports receipts and logs
class ExportLogsJob(BaseJob):
    def __init__(
            self,
            fromBlock,
            toBlock,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter):
        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter
        self.receipt_log_mapper = EthReceiptLogMapper()

        self.blocks = list(range(fromBlock, toBlock))

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.blocks, self._export_logs)

    def _export_logs(self, blocks):
        receipts_rpc = list(generate_get_log_json_rpc(blocks))
        response = self.batch_web3_provider.make_batch_request(json.dumps(receipts_rpc))
        results_batch = rpc_response_batch_to_results(response)
        logs = [self.receipt_log_mapper.json_dict_to_receipt_log(result) for results in results_batch for result in results]
        for log in logs:
            self._export_log(log)

    def _export_log(self, log):
        self.item_exporter.export_item(self.receipt_log_mapper.receipt_log_to_dict(log))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
