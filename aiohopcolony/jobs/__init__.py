import aiohopcolony
from .jobs import *
from .jobs_pipelines import *
from .utils import *

import requests
import yaml
import kubernetes.client
from kubernetes.client.rest import ApiException

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ResourceAlreadyExists(Exception):
    pass


def client(project=None):
    if not project:
        project = aiohopcolony.get_project()
    if not project:
        raise aiohopcolony.ConfigNotFound(
            "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
    if not project.config.project:
        raise aiohopcolony.ConfigNotFound(
            "You have no projects yet. Create one at https://console.hopcolony.io")

    return HopJobs(project)


class HopJobs:
    jobs_cli_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjFQaFFUYTVLZ2dzTlMyTzE0ZkJhazdzZ1lhOW9pc3BhSmJUZUl0c0hmUHcifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJob3AtY29yZSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJjb3JlLWFjY291bnQtdG9rZW4tOGNudmMiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiY29yZS1hY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQudWlkIjoiYWU2NGFhNDUtZDI5YS00MDE4LWEwMzYtMzRiOGY2YTk1YjAwIiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50OmhvcC1jb3JlOmNvcmUtYWNjb3VudCJ9.TSC-Y5u0CK8_urW9Edy5e43yjH0nwaVGR7Zdp0oDIcUTJLwnEewhXP756oWmlclfq15DC-3GDYDwInxWJJKkg-3YqpeZdAtV3BM2EVKVogbn-5TCSAQda0TJGNI2MV6x4sR6RtAaMQmnyIv5NcB0aVcZaQYlDtjsMUR7yPLBwpRqwSX3Jqwh4zV8JmtX9q5s1fW2xPPN-Ze_g6vFF67UVZSWEeb8ygOjWrs14qU1a6Gd2Gc4DVGNfFJQCixPVNSz67PEsgb9Gxxd04SsscTo17s77oaldAAb7nsyOA2HDwYioBokugcBDJNAP795vYmMbTAkVJ2cFSX4tFkCei9BrA"

    def __init__(self, project):
        self.project = project
        self.configuration = kubernetes.client.Configuration()
        self.configuration.host = "https://hopcolony.io:8443"
        self.configuration.verify_ssl = False
        self.configuration.debug = False
        self.configuration.api_key["authorization"] = "Bearer " + \
            self.jobs_cli_token

    def run(self, job, pipelines=[]):
        self.engine = Engine(job, pipelines)
        self.engine.start()

    def deploy(self, name, job, pipelines="", settings={}, schedule=None):
        cfg = aiohopcolony.config()
        spec = job_spec.format(kind="HopCronJob" if schedule else "HopJob",
                               metadata_name=f"{cfg.project}-{name}" if schedule else f"{cfg.project}-job-{name}-{str(time.time()).replace('.', '')}",
                               name=name, schedule=f"schedule: '{schedule}'" if schedule else "",
                               job=job.replace("\n", "\n    "), pipelines=pipelines.replace("\n", "\n    "),
                               settings=yaml.dump(settings).replace("\n", "\n    "), config=yaml.dump(cfg.json).replace("\n", "\n    "))

        with kubernetes.client.ApiClient(self.configuration) as api_client:
            api_instance = kubernetes.client.CustomObjectsApi(api_client)

        # print(cfg.get_namespace())
        try:
            api_instance.create_cluster_custom_object(
                "hopcolony.io", "v1", "hopcronjobs" if schedule else "hopjobs", yaml.load(spec, Loader=yaml.FullLoader))
        except ApiException as e:
            print(e)
            if e.status == 409:
                raise ResourceAlreadyExists
