import aiohopcolony
import aiohopcolony.docs as docs
import aiohopcolony.topics as topics

from .auth_user import *
from .auth_token import *

import re, uuid, time, asyncio
from datetime import datetime
import functools, multiprocessing
from simple_term_menu import TerminalMenu
import subprocess

def client(project = None):
    if not project:
        project = aiohopcolony.get_project()
    if not project:
        raise aiohopcolony.ConfigNotFound("Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
    if not project.config.project:
        raise aiohopcolony.ConfigNotFound("You have no projects yet. Create one at https://console.hopcolony.io")
    
    return HopAuth(project)

class DuplicatedEmail(Exception):
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

    async def select_project(self, user):
        if not len(user.projects):
            return None
        terminal_menu = TerminalMenu(user.projects)
        project_index = terminal_menu.show()
        project =  user.projects[project_index]

        snapshot = await self._docs.index(".hop.projects").where("uuid", isEqualTo=user.uuid).where("name", isEqualTo=project).get()
        if snapshot.success and len(snapshot.docs):
            return aiohopcolony.HopConfig(username = user.email, project = project, token = snapshot.docs[0].source["token"])
        else:
            return None

    async def sign_in_with_hopcolony(self, scopes = []):
        scopes = ','.join(scopes)
        client_id = self.project.config.identity
        
        async def get_response(queue, msg):
            if msg["success"]:
                token = Token(msg["idToken"])
                queue.put(token)
            else:
                queue.put(msg["reason"])
        
        queue = asyncio.Queue()
        callback_with_event = functools.partial(get_response, queue)

        conn = topics.connection()
        await conn.exchange("oauth").topic(client_id).subscribe(callback_with_event, output_type = topics.OutputType.JSON)

        # Open the browser for login
        proc = subprocess.Popen(["firefox", f"https://accounts.hopcolony.io?client_id={client_id}&scope={scopes}"], 
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Wait until there is a login response
        print("Please, login using the browser")

        # Get the response through the queue
        result = await queue.get()

        # Close connection with topics
        conn.close()

        # Parse the response
        if isinstance(result, str):
            return UserSnapshot(None, success=False, reason=result)

        # Close the browser window
        proc.terminate()
        proc.wait()

        # Update or create the user in the database
        ref = self._docs.index(".hop.auth").document(result.payload["uuid"])
        snapshot = await ref.get()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        snapshot = await ref.update({"lastLoginTs": now}) if snapshot.success else \
            await ref.setData({
                "registerTs": now,
                "lastLoginTs": now,
                "provider": result.payload["provider"],
                "uuid": result.payload["uuid"],
                "email": result.payload["email"],
                "name": result.payload["name"],
                "picture": result.payload["picture"],
                "locale": result.payload["locale"],
                "isAnonymous": result.payload["isAnonymous"],
            })
        
        snapshot.doc.source["projects"] = result.payload["projects"]
        user = HopUser.fromJson(snapshot.doc.source)
        return UserSnapshot(user, success=True)
    
    async def register_with_email_and_password(self, email, password, locale = "es"):
        assert email and password, "Email and password can not be empty"
        RESOURCE_ID_NAMESPACE = uuid.UUID('0a7a15ff-aa13-4ac2-897c-9bdf30ce175b')
        uid = str(uuid.uuid5(RESOURCE_ID_NAMESPACE, email))

        snapshot = await self._docs.index(".hop.auth").document(uid).get()
        if snapshot.success:
            raise DuplicatedEmail(f"Email {email} has already been used")

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        doc = {
            "registerTs" : now,
            "lastLoginTs" : now,
            "provider" : "email",
            "uuid" : uid,
            "email" : email,
            "password": password,
            "locale" : locale,
            "isAnonymous": False
        }
        currentUser = None
        snapshot = await self._docs.index(".hop.auth").document(uid).setData(doc)
        if snapshot.success:
            currentUser = HopUser.fromJson(snapshot.doc.source)
        return UserSnapshot(currentUser, success = currentUser is not None)

    async def delete(self, uid):
        return await self._docs.index(".hop.auth").document(uid).delete()