import json
import base64


def base64ToJson(input):
    input_bytes = input.encode('ascii')

    missing_padding = len(input_bytes) % 4
    if missing_padding:
        input_bytes += b'=' * (4 - missing_padding)

    data = base64.b64decode(input_bytes).decode('ascii')
    return json.loads(str(data))

def jsonToBase64(input):
    return base64.b64encode(json.dumps(input).encode('ascii')).decode('ascii')


class Token:
    _header = None
    _payload = None
    _signature = None

    _header = {
        "alg": "RS256",
        "kid": "fed80fec56db99233d4b4f60fbafdbaeb9186c73",
        "typ": "JWT"
    }

    def __init__(self, raw_value):
        self.raw_value = raw_value
        self._i0 = raw_value.index('.')
        self._i1 = raw_value.rfind('.')

    @property
    def header(self):
        return self._header or base64ToJson(self.raw_value[0: self._i0])

    @property
    def payload(self):
        return self._payload or base64ToJson(self.raw_value[self._i0 + 1: self._i1])

    @property
    def signature(self):
        return self._signature or self.raw_value[self._i1 + 1:]

    def __str__(self):
        return self.raw_value

    @classmethod
    def fromData(cls, payload):
        headerRaw = jsonToBase64(cls._header)
        payloadRaw = jsonToBase64(payload)

        return Token(f"{headerRaw}.{payloadRaw}.abc")
