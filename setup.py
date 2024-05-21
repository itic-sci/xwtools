
from setuptools import setup

requirements = [
    'pika>=1.3.1',
    'numpy>=1.21.5',
    'shortuuid>=1.0.11',
    'threadpool>=1.3.2',
    'redis>=4.4.0',
    'elasticsearch>=7.9.1',
    'elasticsearch_dsl>=7.4.0',
    'matplotlib>=3.5.1',
    'jieba>=0.42.1',
    'wheel>=0.37.1',
    'PyMySQL>=1.0.2',
    'pypinyin>=0.47.1',
    'selenium==4.7.2',
    'pymongo>=3.11.0',
    'openpyxl>=3.0.9',
    'pandas>=1.4.2',
    'requests>=2.27.1',
    'beautifulsoup4>=4.10.0',
    'sqlalchemy>=1.4.32',
    'graypy>=2.1.0'
]

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setup(
    name='xwtools',
    version='2.1.0',
    packages=[
        "xwtools",
    ],
    license='BSD License',  # example license
    description='xwtools',
    long_description='这是一个通用的python工具包，帮助你快速的开发项目',
    install_requires=requirements,
    long_description_content_type="text/markdown",
    url='https://github.com/xulehexuwei',
    author='xuwei',
    author_email='15200813194@163.com',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',  # example license
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
)
