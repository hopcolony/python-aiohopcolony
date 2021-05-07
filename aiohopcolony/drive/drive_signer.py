from datetime import datetime
import binascii
import hmac
import hashlib
import urllib.parse


class Signer:
    def __init__(self, host, accessKey, secretKey, region="", algo="AWS4-HMAC-SHA256"):
        self.host = host
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.region = region
        self.algo = algo

    def sign(self, requestType, resource, bodyBytes=b"", queryParameters={}, presigned=False, expires="432000"):
        headers = {}

        now = datetime.utcnow()
        iso8601ts = self.toAwsIso8601(now)
        hPayload = self.hashBinary(bodyBytes)

        if not presigned:
            headers["x-amz-content-sha256"] = hPayload
            headers["x-amz-date"] = iso8601ts

        headers["Host"] = self.host

        rScope = self.requestScope(now, self.region)
        credential = f"{self.accessKey}/{rScope}"
        sHeaders = self.signedHeaders(headers)

        # When it is a presigned request, we must include the components
        # that will go with the query.
        # If not, use the request query components.
        canonicalQuery = {
            "X-Amz-Algorithm": self.algo,
            "X-Amz-Credential": credential,
            "X-Amz-Date": iso8601ts,
            "X-Amz-Expires": expires,
            "X-Amz-SignedHeaders": sHeaders,
        } if presigned else queryParameters

        resource = [urllib.parse.quote(res, safe='')
                    for res in resource.split('/')]

        canonicalRequest = "\n".join([
            requestType,
            "/".join(resource),
            self.canonicalStringFromQuery(canonicalQuery),
            self.canonicalStringFromHeaders(headers),
            sHeaders,
            "UNSIGNED-PAYLOAD" if presigned else hPayload,
        ])

        stringToSign = "\n".join([
            self.algo,
            iso8601ts,
            rScope,
            self.hashedPayload(canonicalRequest),
        ])

        sKey = self.signingKey(now, "s3")
        signature = self._signer(sKey, stringToSign).hex()
        authorization = ",".join([
            f"{self.algo} Credential={credential}",
            f" SignedHeaders={sHeaders}",
            f" Signature={signature}"
        ])

        headers["Authorization"] = authorization

        return SignDetails(headers, self.algo, iso8601ts, expires, f"{self.accessKey}/{rScope}", sHeaders, signature)

    def requestScope(self, t, region):
        dateStr = f"{t.year}{str(t.month).rjust(2, '0')}{str(t.day).rjust(2, '0')}"
        return "/".join([dateStr, region, "s3", "aws4_request"])

    def signedHeaders(self, headers):
        lowerCase = [k.lower() for k in headers.keys()]
        lowerCase.sort()
        return ";".join(lowerCase)

    def canonicalStringFromQuery(self, query):
        keys = list(query.keys())
        keys.sort()
        result = [
            f"{urllib.parse.quote(key, safe = '')}={urllib.parse.quote(query[key], safe = '')}" for key in keys]
        return "&".join(result)

    def canonicalStringFromHeaders(self, headers):
        lowerCase = {k.lower(): k for k in headers.keys()}
        keys = list(lowerCase.keys())
        keys.sort()
        result = [f"{key}:{headers[lowerCase[key]].strip()}" for key in keys]

        # add final blank line
        result.append("")
        return "\n".join(result)

    def getQuerySignature(self, requestType, resource):
        signDetails = self.sign(requestType, resource, presigned=True)
        return f"?{signDetails.flatQuery()}"

    def hashedPayload(self, payload):
        return self.hashBinary(payload.encode('utf-8'))

    def hashBinary(self, payload):
        return hashlib.sha256(payload).hexdigest()

    def signingKey(self, t, service):
        dateStr = f"{t.year}{str(t.month).rjust(2, '0')}{str(t.day).rjust(2, '0')}"
        data2 = self._signer(
            self._signer(f"AWS4{self.secretKey}".encode('utf-8'), dateStr),
            self.region,
        )
        return self._signer(
            self._signer(
                self._signer(
                    self._signer(
                        f"AWS4{self.secretKey}".encode('utf-8'), dateStr),
                    self.region,
                ),
                service,
            ),
            "aws4_request")

    def _signer(self, key, payload):
        return hmac.new(key, msg=payload.encode('utf-8'), digestmod=hashlib.sha256).digest()

    def toAwsIso8601(self, t):
        y = t.year
        m = str(t.month).rjust(2, '0')
        d = str(t.day).rjust(2, '0')
        h = str(t.hour).rjust(2, '0')
        mins = str(t.minute).rjust(2, '0')
        seg = str(t.second).rjust(2, '0')

        return f"{y}{m}{d}T{h}{mins}{seg}Z"


class SignDetails:
    def __init__(self, headers, algo, date, expires, credential, signedHeaders, signature):
        self.headers = headers
        self.algo = algo
        self.date = date
        self.expires = expires
        self.credential = credential
        self.signedHeaders = signedHeaders
        self.signature = signature

    def flatQuery(self):
        query = {
            "X-Amz-Algorithm": self.algo,
            "X-Amz-Credential": self.credential,
            "X-Amz-Date": self.date,
            "X-Amz-Expires": self.expires,
            "X-Amz-SignedHeaders": self.signedHeaders,
            "X-Amz-Signature": self.signature
        }
        data = [
            f"{k}={v if k == 'X-Amz-SignedHeaders' else urllib.parse.quote(v, safe = '')}" for k, v in query.items()]
        return "&".join(data)
