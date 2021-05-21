from dataclasses import dataclass
from .auth_token import Token

@dataclass
class AuthCredential:
    provider: str
    idToken: Token
    email: str = None
    name: str = None
    picture: str = None
    locale: str = None

class GoogleAuthProvider:
    @classmethod
    def credential(cls, idToken: str) -> AuthCredential:
        return GoogleAuthCredential(id = idToken)

class GoogleAuthCredential(AuthCredential):
    def __init__(self, id: str):
        super().__init__(provider = "Google", idToken = Token(id))
        self.email = self.idToken.payload["email"]
        self.name = self.idToken.payload["name"]
        self.picture = self.idToken.payload["picture"]
        self.locale = self.idToken.payload["locale"]

class HopAuthProvider:
    @classmethod
    def credential(cls, idToken: str) -> AuthCredential:
        return HopAuthCredential(id = idToken)

class HopAuthCredential(AuthCredential):
    def __init__(self, id: str):
        super().__init__(provider = "Hopcolony", idToken = Token(id))
        self.email = self.idToken.payload["email"]
        self.name = self.idToken.payload["name"]
        self.picture = self.idToken.payload["picture"]
        self.locale = self.idToken.payload["locale"]
