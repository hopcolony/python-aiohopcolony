import aiohopcolony
import aiohopcolony.docs as docs
import aiohopcolony.topics as topics

from .auth_user import *
from .auth_token import *
from .auth_credentials import *

import uuid
import asyncio
from datetime import datetime
import functools
import subprocess


def client(project=None):
    if not project:
        project = aiohopcolony.get_project()
    if not project:
        raise aiohopcolony.ConfigNotFound(
            "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
    if not project.config.project:
        raise aiohopcolony.ConfigNotFound(
            "You have no projects yet. Create one at https://console.hopcolony.io")

    return HopAuth(project)


class DuplicatedEmail(Exception):
    pass


class HopAuthException(Exception):
    pass


class HopAuth:
    project: aiohopcolony.Project
    _docs: docs.HopDoc

    def __init__(self, project):
        self.project = project
        self._docs = docs.HopDoc(project)

    async def get(self):
        snapshot = await self._docs.index(".hop.auth").get()
        return [HopUser.fromJson(user.source) for user in snapshot.docs]

    def user(self, uuid):
        return UserReference(self._docs, uuid)

    async def sign_in_with_email_and_password(self, email, password) -> AuthResult:
        uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, email))
        snapshot = await self._docs.index(".hop.auth").document(uid).get()
        if snapshot.success:
            if snapshot.doc.source["password"] == password:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                await self._docs.index(".hop.auth").document(uid).update({"lastLoginTs": now})

                user = HopUser.fromJson(snapshot.doc.source)
                self.currentUser = user
                return AuthResult(success=True, user=user)
            return AuthResult(success=False, reason="Incorrect Password")
        return AuthResult(success=False, reason="Email does not exist")

    async def sign_in_with_credential(self, credential: AuthCredential) -> AuthResult:
        uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, credential.email))
        ref = self._docs.index(".hop.auth").document(uid)
        snapshot = await ref.get()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        snapshot = await ref.update({"lastLoginTs": now}) if snapshot.success else \
            await ref.setData({
                "registerTs": now,
                "lastLoginTs": now,
                "provider": credential.provider,
                "uuid": uid,
                "email": credential.email,
                "name": credential.name,
                "picture": credential.picture,
                "locale": credential.locale,
                "isAnonymous": False,
            })

        user = HopUser.fromJson(snapshot.doc.source)
        self.currentUser = user
        return AuthResult(success=True, user=user)

    async def sign_in_with_hopcolony(self, scopes=[]) -> AuthResult:
        scopes = ','.join(scopes)
        client_id = self.project.config.identity

        async def get_response(queue, msg):
            if msg["success"]:
                credential = HopAuthProvider.credential(idToken=msg["idToken"])
                result = await self.signInWithCredential(credential)
                queue.put(result)
            else:
                queue.put(AuthResult(success=False, reason=msg["reason"]))

        queue = asyncio.Queue()
        callback_with_event = functools.partial(get_response, queue)

        conn = topics.connection()
        await conn.exchange("oauth").topic(client_id).subscribe(callback_with_event, output_type=topics.OutputType.JSON)

        # Open the browser for login
        proc = subprocess.Popen(["firefox", f"https://accounts.hopcolony.io?client_id={client_id}&scope={scopes}"],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Wait until there is a login response
        print("Please, login using the browser")

        # Get the response through the queue
        result = await queue.get()

        # Close connection with topics
        conn.close()

        # Close the browser window
        proc.terminate()
        proc.wait()

        # Parse the response
        if not result.success:
            raise HopAuthException(result.reason)

        self.currentUser = result.user

        return result

    async def register_with_email_and_password(self, email, password, locale="es") -> AuthResult:
        assert email and password, "Email and password can not be empty"
        uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, email))

        snapshot = await self._docs.index(".hop.auth").document(uid).get()
        if snapshot.success:
            raise DuplicatedEmail(f"Email {email} has already been used")

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        doc = {
            "registerTs": now,
            "lastLoginTs": now,
            "provider": "email",
            "uuid": uid,
            "email": email,
            "password": password,
            "locale": locale,
            "isAnonymous": False
        }
        currentUser = None
        snapshot = await self._docs.index(".hop.auth").document(uid).setData(doc)
        if snapshot.success:
            currentUser = HopUser.fromJson(snapshot.doc.source)
        return AuthResult(success=True, user = currentUser)

    async def delete(self, uid):
        return await self._docs.index(".hop.auth").document(uid).delete()
