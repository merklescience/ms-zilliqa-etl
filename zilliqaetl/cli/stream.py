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
import random

import click

from zilliqaetl.cli.rate_limiting_proxy import RateLimitingProxy
from zilliqaetl.exporters.zilliqa_item_exporter import get_streamer_exporter
from zilliqaetl.streaming.zil_stream_adapter import ZilliqaStreamerAdapter
from zilliqaetl.thread_local_proxy import ThreadLocalProxy
from zilliqaetl.enumeration.entity_type import EntityType
from pyzil.zilliqa.api import ZilliqaAPI


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-block-file', default='last_synced_block.txt', type=str, help='')
@click.option('--lag', default=0, type=int, help='The number of blocks to lag behind the network.')
@click.option('-p', '--provider-uri', default='https://dev-api.zilliqa.com/', type=str,
              help='The URI of the web3 provider e.g. '
                   'https://dev-api.zilliqa.com/')
@click.option('-o', '--output', type=str,
              help='Google PubSub topic path e.g. projects/your-project/topics/zilliqa_blockchain. '
                   'If not specified will print to console')
@click.option('-s', '--start-block', default=None, type=int, help='Start block')
@click.option('-e', '--entity-types', default=','.join(EntityType.ALL_FOR_STREAMING), type=str,
              help='The list of entity types to export.')
@click.option('--period-seconds', default=10, type=int, help='How many seconds to sleep between syncs')
@click.option('-B', '--block-batch-size', default=1, type=int, help='How many blocks to batch in single sync round')
@click.option('-w', '--max-workers', default=5, type=int, help='The number of workers')
@click.option('--pid-file', default=None, type=str, help='pid file')
@click.option('-r', '--rate-limit', default=None, show_default=True, type=int,
              help='Maximum requests per second for provider in case it has rate limiting')
def stream(last_synced_block_file, lag, provider_uri, output, start_block, entity_types,
           period_seconds=10,  block_batch_size=10, max_workers=5, pid_file=None, rate_limit=20):
    """Streams all data types to console or Google Pub/Sub."""
    zilliqa_api = ThreadLocalProxy(lambda: ZilliqaAPI(provider_uri))
    if rate_limit is not None and rate_limit > 0:
        zilliqa_api = RateLimitingProxy(zilliqa_api, max_per_second=rate_limit)

    entity_types = parse_entity_types(entity_types)

    from zilliqaetl.exporters.zilliqa_item_exporter import get_item_exporter
    from blockchainetl.streaming.streamer import Streamer

    logging.info('Using ' + provider_uri)
    zil_streamer_adapter = ZilliqaStreamerAdapter(
        provider_uri=zilliqa_api, item_exporter=get_streamer_exporter(output), max_workers=max_workers)

    streamer = Streamer(
        blockchain_streamer_adapter=zil_streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        pid_file=pid_file
    )
    streamer.stream()


def parse_entity_types(entity_types):
    entity_types = [c.strip() for c in entity_types.split(',')]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                '--entity-type', '{} is not an available entity type. Supply a comma separated list of types from {}'
                .format(entity_type, ','.join(EntityType.ALL_FOR_STREAMING)))

    return entity_types
