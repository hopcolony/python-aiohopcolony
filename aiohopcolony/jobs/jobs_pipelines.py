import aiohopcolony
from aiohopcolony import docs


class JobPipeline:
    name = None

    def __init__(self, job, *args, **kwargs):
        # Set the input args as attributes to the job
        self.__dict__.update(kwargs)

        self.logger = job.logger


class Stdout(JobPipeline):
    name = "stdout"

    def process_item(self, item, job):
        self.logger.info(item)
        return item


class HopDocs(JobPipeline):
    name = "hop-docs"

    def __init__(self, *args, **kwargs):
        super(HopDocs, self).__init__(*args, **kwargs)
        aiohopcolony.initialize()
        self.db = docs.client()

    def process_item(self, item, job):
        try:
            index = item["index"]
            source = item["source"]
        except KeyError as e:
            self.logger.error(
                f"[{self.name} pipeline] {str(e)} key not present in yielded item")

        try:
            id = item["id"]
            response = self.db.index(index).document(id).setData(source)
        except KeyError:
            response = self.db.index(index).add(source)

        if not response.success:
            count_label = f"[{item['count']}]" if "count" in item else ""
            self.logger.error(
                f"[{self.name} pipeline]{count_label} Insertion not succeded: {response.reason}. Item: {source}")
        elif "count" in item:
            self.logger.info(f"[{item['count']}] Inserted Successfully")

        return item
