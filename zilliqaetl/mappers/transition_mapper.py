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

from zilliqaetl.utils.zilliqa_utils import to_int, encode_bench32_address


def convert_params(elems):
    di_obj = {"from": None, "to": None, "params_amount": "0", "initiator": None}
    for elem in elems:
        vname = elem["vname"]
        val = elem["value"]
        if vname in ["from", "sender"] and di_obj.get("from") is None:
            # if di_obj.get("from") is None:  # not needed val is not None
            di_obj["from"] = val
        elif vname in ["to", "recipient"] and di_obj.get("to") is None:
            # if di_obj.get("to") is None:  # not needed val is not None
            di_obj["to"] = val
        elif vname == 'amount':  # not needed val is not None
            # if val is not None:
            di_obj["params_amount"] = val
        elif vname == 'initiator':  # not needed val is not None
            # if val is not None:
            di_obj["initiator"] = val
    return di_obj


def map_transitions(tx_block, txn):
    receipt = txn.get('receipt')
    if receipt and receipt.get('transitions'):
        for index, transition in enumerate(receipt.get('transitions')):
            msg = transition.get('msg')
            yield {
                'type': 'transition',
                'block_number': tx_block.get('number'),
                'block_timestamp': tx_block.get('timestamp'),
                'transaction_id': txn.get('ID'),
                'index': index,
                'accepted': receipt.get('accepted'),
                'addr': encode_bench32_address(transition.get('addr')),
                'depth': transition.get('depth'),
                'amount': to_int(msg.get('_amount')),
                'recipient': encode_bench32_address(msg.get('_recipient')),
                'tag': msg.get('_tag'),
                **convert_params(msg.get('params'))
            }


def map_token_traces(tx_block, txn, txn_type):
    # TODO :confirm logic once
    receipt = txn.get('receipt')
    if receipt and receipt.get('transitions'):
        for index, transition in enumerate(receipt.get('transitions')):
            msg = transition.get('msg')
            data = {
                "type": txn_type,
                'block_number': tx_block.get('number'),
                'block_timestamp': tx_block.get('timestamp'),
                'transaction_id': '0x' + txn.get('ID'),
                'log_index': index,
                'from_address': encode_bench32_address(transition.get('addr')),
                'msg_amount': to_int(msg.get('_amount')),
                'to_address': encode_bench32_address(msg.get('_recipient')),
                'call_type': msg.get('_tag'),
                "receipt_status": int(receipt.get("success")),
                **convert_params(msg.get('params'))
            }
            if txn_type == 'trace' and int(txn.get("amount", 0)) > 0:
                yield data
            elif txn_type == 'token_transfer' and (
                    (data.get("msg_amount", 0) > 0) or data.get("params_amount", "0") != "0"):
                yield data
            # else:
            #     yield {}
