from pyzil.zilliqa.api import ZilliqaAPI

class ZilliqaStreamerAdapter:
    def __init__(self, provider_uri, item_exporter):
        self.provider_uri = provider_uri
        self.item_exporter = item_exporter
        self.api = ZilliqaAPI(provider_uri)

    def open(self):
        self.item_exporter.open()
        pass

    def get_current_block_number(self):
        return int(self.api.GetLatestDsBlock()['header']['BlockNum'])

    def export_all(self, start_block, end_block):
        pass

    def close(self):
        self.item_exporter.close()


if "__main__" == __name__:
    _streamer = ZilliqaStreamerAdapter("https://dev-api.zilliqa.com/",None)
    print(_streamer.get_current_block_number())