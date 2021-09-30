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


from zilliqaetl.utils.zilliqa_utils import to_int, encode_bech32_pub_key, encode_bech32_address


# Modified acc to MS use case
def map_transaction(tx_block, txn):
    block = {
        'type': 'transaction',
        'hash': '0x' + txn.get('ID'),
        'block_number': tx_block.get('number'),
        'block_timestamp': tx_block.get('timestamp'),
        'value': to_int(txn.get('amount')),
        'gas_price': to_int(txn.get('gasPrice')),
        'from_address': encode_bech32_pub_key(txn.get('senderPubKey')),
        'to_address': encode_bech32_address(txn.get('toAddr')),
        **map_receipt(txn)
    }
    block["fee"] = block.pop("gas_price") * block.pop("gas_used")
    if block["receipt_status"] == 0:
        block["value"] = 0
        block["hash"]="0x"
    return block


def map_receipt(txn):
    receipt = txn.get('receipt')
    if receipt is None:
        return None

    return {
        'receipt_status': int(receipt.get('success')),
        'gas_used': to_int(receipt.get('cumulative_gas'))
    }
