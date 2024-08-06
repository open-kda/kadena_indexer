import base64
import hashlib

def _ensure_bytes(x):
    if isinstance(x, str):
        return x.encode('ascii')
    return x

def b64_encode(data):
    """ Encode data (str ot bytes) to Base 64 """
    data = _ensure_bytes(data)
    encoded = base64.urlsafe_b64encode(data).rstrip(b'=')
    return encoded.decode('ascii')

_PADDING_TABLE = [b"", b"===", b"==", b"="]

def b64_decode(data):
    """ Decode a base64 string to bytes """
    data = _ensure_bytes(data)
    padding = _PADDING_TABLE[len(data)%4]
    return base64.urlsafe_b64decode(data+padding)

def k_hash(data):
    """ Do a kadena compatible Hash: Blake2b """
    hs = hashlib.blake2b(digest_size=32)
    hs.update(data)
    return hs.digest()

def k_hash_b64(data):
    """ Do a Pact/Kadena comptaible Hash: Blak2b => Base64 """
    return b64_encode(k_hash(data))
