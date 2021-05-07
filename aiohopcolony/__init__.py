import os
import json
import base64
import yaml
import aiofile
from pathlib import Path

_project = None


class ConfigNotFound(Exception):
    pass


class InvalidConfig(Exception):
    pass


class HopConfig:
    hop_dir = str(Path.home()) + "/.hop"
    hop_file = os.path.join(hop_dir, "config")
    username = None
    namespace = None
    project = None
    token = None
    identity = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.identity = self.compute_identity()

    def get_namespace(self):
        ns = 'a'+base64.b64encode(self.username.encode('ascii')).decode('ascii').lower().replace("=", "-")+'a' \
            if not self.namespace else self.namespace
        return str(ns)

    def compute_identity(self):
        if not (self.username or self.namespace) or not self.project:
            return None

        raw = self.get_namespace() + "." + str(self.project)
        message_bytes = raw.encode('ascii')
        return base64.b64encode(message_bytes).decode('ascii')

    @property
    def valid(self):
        return (self.username or self.namespace) and self.project and self.token and self.identity

    @classmethod
    async def update(cls, **kwargs):
        try:
            current = await cls.fromFile(cls.hop_file).json
            for key, value in kwargs.items():
                if value:
                    current[key] = value
            return cls.fromJson(**current)
        except FileNotFoundError:
            return cls(**kwargs)

    @classmethod
    async def fromFile(cls, file):
        async with aiofile.async_open(file, "r") as f:
            config = yaml.load(await f.read(), Loader=yaml.FullLoader)
            return cls.fromJson(**config)

    @classmethod
    def fromJson(cls, **kwargs):
        return cls(**kwargs)

    @property
    def json(self):
        data = {"project": self.project,
                "token": self.token}
        if self.username:
            data["username"] = self.username
        else:
            data["namespace"] = self.namespace
        return data

    @property
    def yaml(self):
        return yaml.dump(self.json)

    def commit(self):
        Path(self.hop_dir).mkdir(parents=False, exist_ok=True)
        with open(self.hop_file, "w") as f:
            f.write(self.yaml)
        return self.json


class Project:
    config: HopConfig

    @classmethod
    async def create(cls, username=None, project=None, token=None, namespace=None,
                     config_file=".hop.config"):

        self = Project()

        # Use username, project and token values if the 3 provided
        if any([username, namespace, project, token]):
            if all([username, project, token]):
                self.config = HopConfig(
                    username=username, project=project, token=token)
                return self
            elif all([namespace, project, token]):
                self.config = HopConfig(
                    namespace=namespace, project=project, token=token)
                return self
            else:
                raise InvalidConfig(
                    "If you provide one of [username, project, token] or [namespace, project, token], you need to provide the 3 of them")

        # Else, use the provided config_file if provided. If not, try ~/.hop/config
        try:
            self.config = await HopConfig.fromFile(config_file)
        except FileNotFoundError:
            if config_file != HopConfig.hop_dir:
                try:
                    self.config = await HopConfig.fromFile(HopConfig.hop_file)
                except FileNotFoundError:
                    raise ConfigNotFound(
                        "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
            else:
                raise ConfigNotFound(
                    "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
        except IsADirectoryError:
            raise ConfigNotFound(
                f"Config file provided [{config_file}] is a directory")

        return self

    @property
    def name(self):
        return self.config.project


async def initialize(**kwargs):
    global _project
    _project = await Project.create(**kwargs)
    return _project


def get_project():
    return _project


def config():
    return _project.config if _project else None
