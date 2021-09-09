# MIT License
#
# Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
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

from pyzil.zilliqa.api import ZilliqaAPI
from zilliqaetl.enumeration.entity_type import EntityType
from zilliqaetl.jobs.export_tx_blocks_job import ExportTxBlocksJob


class ZilliqaStreamerAdapter:
    def __init__(self, provider_uri, item_exporter):
        self.provider_uri = provider_uri
        self.item_exporter = item_exporter
        self.api = ZilliqaAPI(provider_uri)

    def open(self):
        self.item_exporter.open()
        pass

    def get_current_block_number(self):
        return int(self.api.GetLatestTxBlock()['header']['BlockNum'])

    def export_all(self, start_block, end_block):
        job = ExportTxBlocksJob(
            start_block=start_block,
            end_block=end_block,
            zilliqa_api=self.api,
            max_workers=5,
            export_exceptions=False,
            export_event_logs=False,
            export_transactions=True,
            export_traces=True,
            export_token_transfers=True,
            export_transitions=True,
            item_exporter=self.item_exporter,
        )
        job.run()

    def close(self):
        self.item_exporter.close()


if "__main__" == __name__:
    _streamer = ZilliqaStreamerAdapter("https://dev-api.zilliqa.com/", None)
    print(_streamer.get_current_block_number())
    print(_streamer.export_all(3225427, 3225438))
