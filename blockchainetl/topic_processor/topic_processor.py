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


import logging
import os
import time

from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from blockchainetl.file_utils import smart_open
from processor import get_one_message, commit_message, connect_kafka_consumer

class TopicProcessor:
    def __init__(
            self,
            blockchain_streamer_adapter=StreamerAdapterStub(),
            last_synced_block_file='last_synced_block.txt',
            lag=0,
            period_seconds=10,
            block_batch_size=10,
            retry_errors=True,
            pid_file=None,
            health_probe_file="health_probe"):
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file
        self.health_probe_file = health_probe_file
        self.consumer = connect_kafka_consumer(kafkaUrl=os.environ['KAFKA_URL'], topic=os.environ['KAFKA_TOPIC'])

    def stream(self):
        try:
            if self.pid_file is not None:
                logging.info('Creating pid file {}'.format(self.pid_file))
                write_to_file(self.pid_file, str(os.getpid()))
            self.blockchain_streamer_adapter.open()
            while True:
                init_block = self.last_synced_block + 1
                end_block = self.blockchain_streamer_adapter.get_current_block_number() - self.lag
                while True:
                    message = get_one_message(self.consumer)
                    init_block = int(message['init_block'])
                    end_block = int(message['end_block'])
                    self._do_stream(init_block, end_block)
        finally:
            self.blockchain_streamer_adapter.close()
            if self.pid_file is not None:
                logging.info('Deleting pid file {}'.format(self.pid_file))
                delete_file(self.pid_file)

    def _do_stream(self, init_block, end_block):
        fail_count = 0
        while True and (end_block is None or self.last_synced_block < end_block):
            synced_blocks = 0

            try:
                synced_blocks = self._sync_cycle(init_block, end_block)
                update_health_probe(self.health_probe_file, "OK")
                fail_count = 0
            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logging.exception('An exception occurred while syncing block data.')
                fail_count += 1
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                logging.info('Nothing to sync. Sleeping for {} seconds...\n Fail count is {}'.format(self.period_seconds, fail_count))
                time.sleep(self.period_seconds)
            
            if fail_count >= 10:
                logging.warning('Too many errors. Updating Health Probe...')
                update_health_probe(self.health_probe_file, "FAIL")

    def _sync_cycle(self, init_block, end_block):
        current_block = self.blockchain_streamer_adapter.get_current_block_number()

        blocks_to_sync = max(end_block - init_block, 0)

        logging.info('Current block {}, target block {}, init block {}, blocks to sync {}'.format(
            current_block, end_block, init_block, blocks_to_sync))

        if blocks_to_sync != 0:
            self.blockchain_streamer_adapter.export_all(init_block + 1, end_block)
            logging.info('Exported block {}'.format(end_block))
            self.last_synced_block = end_block

        return blocks_to_sync


def delete_file(file):
    try:
        os.remove(file)
    except OSError:
        pass

def update_health_probe(file, status):
    write_to_file(file, status + '\n')
    if status == "FAIL":
        delete_file(file)

def write_to_file(file, content):
    with smart_open(file, 'w') as file_handle:
        file_handle.write(content)
