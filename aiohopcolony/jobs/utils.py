default_settings = """#### Hop Jobs Settings ####

# Configure your jobs:
example:
  # Arguments to the job
  args:
    initial_page: 2
  pipelines:
    - to-json:
        # If it's a custom pipeline, set custom to true
        custom: true
        # Args to the pipeline
        project: {}
    # If not, it will be searched in the built-in pipelines
    - stdout
  requirements:
    # numpy

"""

example_pipeline = """from aiohopcolony import jobs

class ToJsonPipeline(jobs.JobPipeline):
    name = "to-json"

    def process_item(self, item, job):
        return {"project": self.project, "data": item}"""

example_job = """from aiohopcolony import jobs

class ExampleJob(jobs.Job):
    name = "example"
    def __init__(self, *args, **kwargs):
        super(ExampleJob, self).__init__(*args, **kwargs)
        self.entrypoint = f"https://news.ycombinator.com/news?p={self.initial_page}"

    def parse(self, response):
        yield from response.css('a.storylink::text').getall()

        next_page = response.css('a.morelink::attr(href)').get()
        if next_page is not None:
            response.follow(next_page)"""

job_spec = """apiVersion: hopcolony.io/v1
kind: {kind}
metadata:
  name: {metadata_name}
spec:
  name: {name}
  {schedule}
  job: |-
    {job}
  
  pipelines: |-
    {pipelines}

  settings:
    {settings}
  
  config:
    {config}
"""
