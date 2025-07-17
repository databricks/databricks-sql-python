import uuid
import logging

logger = logging.getLogger(__name__)


def guid_to_hex_id(guid: bytes) -> str:
    """Return a hexadecimal string instead of bytes

    Example:
        IN   b'\x01\xee\x1d)\xa4\x19\x1d\xb6\xa9\xc0\x8d\xf1\xfe\xbaB\xdd'
        OUT  '01ee1d29-a419-1db6-a9c0-8df1feba42dd'

    If conversion to hexadecimal fails, a string representation of the original
    bytes is returned
    """

    try:
        this_uuid = uuid.UUID(bytes=guid)
    except Exception as e:
        logger.debug("Unable to convert bytes to UUID: %r -- %s", guid, str(e))
        return str(guid)
    return str(this_uuid)
