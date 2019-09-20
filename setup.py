import setuptools
import codecs

NAME = "ena-refget-scheduler"
VERSION = "0.1.0"
AUTHOR = "Jeremy Adams"
EMAIL = "jeremy.adams@ga4gh.org"

try:
    codecs.lookup('mbcs')
except LookupError:
    ascii = codecs.lookup('ascii')
    func = lambda name, enc=ascii: {True: enc}.get(name=='mbcs')
    codecs.register(func)

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = [
    "click"
]

setuptools.setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    description="Schedule the processing of ENA sequences and upload to S3 "
        + "public dataset",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ga4gh/ena-refget-scheduler",
    package_data={'': ['*.ini']},
    packages=setuptools.find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            'ena-refget-scheduler=ga4gh.refget.ena.cli.entrypoint:main',
        ]
    }
    ,
    classifiers=(
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Natural Language :: English",
        "Topic :: Scientific/Engineering :: Bio-Informatics"
    ),
)
