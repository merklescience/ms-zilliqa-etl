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
    print(_streamer.export_all(3225427,3225438))
