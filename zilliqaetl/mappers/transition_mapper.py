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
import json

from pyzil.crypto import zilkey

from zilliqaetl.utils.zilliqa_utils import to_int, json_dumps, iso_datetime_string, encode_bech32_address, \
    encode_bech32_address

SUPPORTED_TOKENS = ["zil1zu72vac254htqpg3mtywdcfm84l3dfd9qzww8t", "zil180v66mlw007ltdv8tq5t240y7upwgf7djklmwh",
                    "zil1cuf78e3p37utekgk0gtcvd3hvkrqcgt06lrnty"]


def params_to_json(elems, json_string=True):
    di_obj = {}
    for elem in elems:
        # elem = json.loads(elem)
        if isinstance(elem["value"], str):
            if zilkey.is_valid_address(elem["value"]):
                val = encode_bech32_address(elem["value"])
            else:
                val = None
            di_obj[elem["vname"]] = val if val is not None else elem["value"]
        else:
            di_obj[elem["vname"]] = elem["value"]
    if json_string:
        return json.dumps(di_obj) if len(di_obj) > 0 else None
    else:
        return di_obj if len(di_obj) > 0 else None


def map_transitions(tx_block, txn):
    receipt = txn.get('receipt')
    if receipt and receipt.get('transitions'):
        for index, transition in enumerate(receipt.get('transitions')):
            msg = transition.get('msg')
            yield {
                'type': 'transition',
                'token_address': '0x0000',
                'block_number': tx_block.get('number'),
                'block_timestamp': tx_block.get('timestamp'),
                'transaction_id': txn.get('ID'),
                # 'index': index,
                # 'accepted': receipt.get('accepted'),
                'addr': encode_bech32_address(transition.get('addr')),
                # 'depth': transition.get('depth'),
                'amount': msg.get('_amount'),
                'recipient': encode_bech32_address(msg.get('_recipient')),
                # 'tag': msg.get('_tag'),
                # 'params': [json_dumps(param) for param in msg.get('params')],
            }


def map_token_traces(tx_block, txn):
    # TODO: Cleanup logic for adding 0x for token_transfers, only for this we have to add rest we are not adding
    txn_type = 'token_transfer'
    receipt = txn.get('receipt')
    if receipt and receipt.get('transitions'):
        for index, transition in enumerate(receipt.get('transitions')):
            msg = transition.get('msg')
            encoded_addr = encode_bech32_address(transition.get('addr'))
            encoded_recipient = encode_bech32_address(msg.get('_recipient'))
            data = {
                "type": txn_type,
                'block_number': tx_block.get('number'),
                'block_timestamp': tx_block.get('timestamp'),
                'transaction_hash': '0x' + txn.get('ID'),
                # 'log_index': index,
                # "receipt_status": int(receipt.get("success")),
            }
            # if txn_type == 'trace' and msg.get('_amount', "0") != "0":
            #     data["value"] = msg.get('_amount')
            #     data["from_address"] = encoded_addr
            #     data["to_address"] = encoded_recipient
            #     yield data
            if (msg.get('_amount', "0") == "0") and (
                    (encoded_addr in SUPPORTED_TOKENS) or (encoded_recipient in SUPPORTED_TOKENS)):
                tag = msg.get('_tag')
                params = params_to_json(msg.get('params'), json_string=False)
                param_keys = set(params.keys())
                if (tag == "TransferFrom") and ({"from", "to", "amount"} <= param_keys):
                    data["from_address"] = params["from"]
                    data["to_address"] = params["to"]
                    data["value"] = params["amount"]
                    data["token_address"] = encoded_recipient
                    # data["call_type"] = tag
                    yield data
                elif (tag == "RecipientAcceptTransfer") and ({"sender", "recipient", "amount"} <= param_keys):
                    data["from_address"] = params["sender"]
                    data["to_address"] = params["recipient"]
                    data["value"] = params["amount"]
                    data["token_address"] = encoded_addr
                    # data["call_type"] = tag
                    yield data
                elif (tag == "Mint") and ("amount" in param_keys) and \
                        (("to" in param_keys) or ("recipient" in param_keys)):
                    data["from_address"] = "zil1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq9yf6pz"
                    data["to_address"] = params.get("to", params.get("recipient", None))
                    data["value"] = params["amount"]
                    data["token_address"] = encoded_addr if (encoded_addr in SUPPORTED_TOKENS) else encoded_recipient
                    # data["call_type"] = tag
                    yield data
                elif (tag == "Burn") and ("amount" in param_keys) and \
                        (("to" in param_keys) or ("recipient" in param_keys)):
                    data["from_address"] = params.get("initiator", encoded_addr)
                    data["to_address"] = "zil1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq9yf6pz"
                    data["value"] = params["amount"]
                    data["token_address"] = encoded_addr if (encoded_addr in SUPPORTED_TOKENS) else encoded_recipient
                    # data["call_type"] = tag
                    yield data
