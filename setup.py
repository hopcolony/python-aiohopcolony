from os import path
from setuptools import setup, find_packages

about_path = path.join(path.dirname(path.abspath(__file__)), 'aiohopcolony', '__about__.py')
about = {}
with open(about_path) as fp:
    exec(fp.read(), about)

setup(
    name=about['__title__'],
    version=about['__version__'],
    description='Asyncio HopColony Core Python SDK',
    long_description='Asyncio HopColony SDK to communicate with backend for Python developers',
    url=about['__url__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    license=about['__license__'],
    keywords='hopcolony core cloud development backend asyncio',
    install_requires=[
        "aiofile==3.5.0",
        "aiohttp==3.7.4",
        "pyyaml==5.4.1",
        "pika==1.2.0",
        "beautifulsoup4==4.9.3"
    ],
    packages=find_packages(),
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.9',
        'License :: OSI Approved :: MIT License',
    ],
)