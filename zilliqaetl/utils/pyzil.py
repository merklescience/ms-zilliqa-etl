from utils import bech32
from utils import pyzil_utils


def to_int(val):
    if val is None:
        return val
    if isinstance(val, str):
        return int(val)

    return val


ADDRESS_NUM_BYTES = 20
ADDRESS_STR_LENGTH = ADDRESS_NUM_BYTES * 2


def is_valid_address(address: str) -> bool:
    """Return True if address is valid."""
    if address.lower().startswith("0x"):
        address = address[2:]
    if len(address) != ADDRESS_STR_LENGTH:
        return False
    # noinspection PyBroadException
    try:
        pyzil_utils.hex_str_to_int(address)
    except Exception:
        return False
    return True


def encode_bech32_address(address):
    """Convert 20 bytes address to bech32 address."""
    if address is None:
        return None
    if not is_valid_address(address):
        return None
    return bech32.encode("zil", pyzil_utils.hex_str_to_bytes(address))
