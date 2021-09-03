import json
from datetime import datetime
from utils import bech32, pyzil_utils
from utils.pyzil import is_valid_address
from utils import schnorr
import logging


def to_int(val):
    if val is None:
        return val
    if isinstance(val, str):
        return int(val)

    return val


def iso_datetime_string(timestamp):
    if timestamp is None:
        return None
    if isinstance(timestamp, str):
        timestamp = int(timestamp)

    return datetime.utcfromtimestamp(timestamp / 1000000).strftime('%Y-%m-%d %H:%M:%S')


def encode_bench32_pub_key(pub_key):
    logging.info("Pub Key Step 1 " + pub_key)
    if pub_key is None:
        return None
    if isinstance(pub_key, str):
        pub_key = pyzil_utils.hex_str_to_bytes(pub_key)
    logging.info("Pub Key Step 2 " + pub_key)
    pub_key = schnorr.decode_public(pub_key)
    logging.info("Pub Key Step 3 " + pub_key)
    pub_key = schnorr.encode_public(pub_key.x, pub_key.y)
    logging.info("Pub Key Step 4 " + pub_key)
    pub_key = pyzil_utils.hash256_bytes(pub_key)
    logging.info("Pub Key Step 5 " + pub_key)
    pub_key = pyzil_utils.bytes_to_hex_str(pub_key)[-pyzil_utils.ADDRESS_STR_LENGTH:]
    logging.info("Pub Key Step 6 " + pub_key)
    return pub_key


def encode_bench32_address(address):
    if address is None:
        return None
    if not is_valid_address(address):
        return None
    return bech32.encode("zil", pyzil_utils.hex_str_to_bytes(address))


# def json_dumps(obj):
#     if obj is None:
#         return None
#     return json.dumps(obj, separators=(',', ':'))
