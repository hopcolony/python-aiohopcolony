import logging
import requests
import re
from itertools import cycle
from urllib.parse import urlparse
from parsel import Selector
from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium import common
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
import sys
sys.setrecursionlimit(2000)


class Job:
    entrypoint = None
    selenium = False

    def __init__(self, *args, **kwargs):
        # Set the input args as attributes to the job
        self.__dict__.update(kwargs)

        self.logger = logging.getLogger(self.name)
        logging.basicConfig(level=logging.INFO, format='%(message)s')

    def write(self, data, name=None, extension="html"):
        name = name if name is not None else self.page
        with open(f"{name}.{extension}", "w") as f:
            data = json.dumps(data) if extension == "json" else data
            f.write(data)


class Engine:
    last_gets = ["a", "b", "c", "d"]
    proxies = cycle(
        ["3.239.28.97:8080", "3.238.35.140:8080", "174.129.147.226:8080"])
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"}

    def __init__(self, job, pipelines):
        self.job = job
        self.pipelines = pipelines
        assert job.entrypoint, "Please provide an entrypoint"

    def do_captcha(self, driver):
        try:
            wait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it(
                driver.find_element_by_xpath("/html/body/iframe")))
            wait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it(
                driver.find_element_by_xpath("//*[@id='captcha-submit']/div/div/iframe")))

            captcha = driver.find_element_by_xpath(
                "//*[@id='recaptcha-anchor']")
            ActionChains(driver).move_to_element(captcha).click().perform()
        except common.exceptions.NoSuchElementException as e:
            print(e)
            pass
        return False

    def get(self, url, callback=None):
        # Check you are not following the url you visited the last 3 times
        if isinstance(url, str):
            self.last_gets.pop(0)
            self.last_gets.append(url)
            assert all(
                x == self.last_gets[0] for x in self.last_gets) == False, f"You're trying to follow an url you visited the last {len(self.last_gets)-1} times: {url}"

        # Assign different callback if provided
        parse_callback = self.job.parse
        if callback:
            parse_callback = callback

        try:
            if not self.job.selenium:
                # data = requests.get(url, headers = self.headers, proxies = {"http": self.proxy})
                data = requests.get(url, headers=self.headers)
                response = RESTJobResponse(self, data)
            else:
                if isinstance(url, str):
                    self.browser.get(url)
                else:
                    ActionChains(self.browser).click(url).perform()
                response = SeleniumJobResponse(self)
                # Check captcha
                # if self.do_captcha(self.browser):
                #     return
        except requests.exceptions.ConnectionError:
            self.job.logger.error(f"Could not get a connection with \"{url}\"")
            return
        except common.exceptions.WebDriverException as e:
            print(e)
            self.browser.close()
            return

        # Rotate proxy
        self.proxy = next(self.proxies)

        try:
            for item in parse_callback(response):
                # Send to pipelines
                for pipeline in self.pipelines:
                    item = pipeline.process_item(item, self.job)
        except TypeError as e:
            # Probably returning nothing from parse method
            pass
        except Exception as e:
            print(e)

    headless = False
    options = None
    profile = None
    capabilities = None

    # def setUpProfile(self):
    #     self.profile = FirefoxProfile()
    #     self.profile._install_extension("buster_captcha_solver_for_humans-1.1.0-an+fx.xpi", unpack=False)
    #     self.profile.set_preference("security.fileuri.strict_origin_policy", False)
    #     self.profile.update_preferences()

    # def setUpOptions(self):
    #     self.options = FirefoxOptions()
    #     # self.options.headless = self.headless

    # def setUpCapabilities(self):
    #     self.capabilities = DesiredCapabilities.FIREFOX
    #     self.capabilities['marionette'] = True

    # def setUpProxy(self):
    #     self.proxy = next(self.proxies)
    #     self.capabilities['proxy'] = { "proxyType": "MANUAL", "httpProxy": self.proxy, "ftpProxy": self.proxy, "sslProxy": self.proxy }

    def start(self):
        if self.job.selenium:
            # self.setUpProfile()
            # self.setUpOptions()
            # self.setUpCapabilities()
            # self.setUpProxy()

            # self.browser = Firefox(options=self.options, capabilities=self.capabilities,
            #         firefox_profile=self.profile, executable_path=GeckoDriverManager().install())
            self.browser = Chrome(ChromeDriverManager().install())
        self.get(self.job.entrypoint)


class RESTJobResponse:
    def __init__(self, engine, response):
        self.engine = engine
        self.url = response.url
        self.response = response
        parsed_uri = urlparse(self.url)
        self.base = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
        self.status = response.status_code
        self.raw = response.text
        self.selector = Selector(text=self.raw)

    def css(self, query):
        return self.selector.css(query)

    def xpath(self, query):
        return self.selector.xpath(query)

    def get(self, url, params={}):
        data = requests.get(url, params=params)
        return RESTJobResponse(self, data)

    def post(self, url, json={}):
        data = requests.post(url, json=json)
        return RESTJobResponse(self, data)

    def follow(self, endpoint, callback=None):
        if endpoint is None:
            return
        url = self.base + endpoint
        if re.match(r"http.*", endpoint):
            url = endpoint
        self.engine.get(url, callback)


class SeleniumJobResponse:
    def __init__(self, engine):
        self.engine = engine
        self.url = engine.browser.current_url
        parsed_uri = urlparse(self.url)
        self.base = f"{parsed_uri.scheme}://{parsed_uri.netloc}/"
        self.raw = engine.browser.page_source

    @property
    def driver(self):
        return self.engine.browser

    def xpath(self, query):
        return self.driver.find_elements_by_xpath(query)

    def css(self, selector):
        return self.driver.find_elements_by_css_selector(selector)

    def tag(self, tag):
        return self.driver.find_element_by_tag_name(tag)

    def click(self, element):
        ActionChains(self.driver).click(element).perform()

    def click_and_call(self, element, callback=None):
        self.engine.get(element, callback)

    def move_to_element(self, element):
        ActionChains(self.driver).move_to_element(element).perform()

    def go_to_end(self):
        self.driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

    def follow(self, endpoint, callback=None):
        url = self.base + endpoint
        if re.match(r"http.*", endpoint):
            url = endpoint
        self.engine.get(url, callback)

    def close(self):
        self.engine.browser.close()
