[![Python 3.6](https://img.shields.io/badge/python-3.6%20|%203.7-blue.svg?style=flat-square)](https://www.python.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)
[![Travis (.org) branch](https://img.shields.io/travis/ga4gh/refget-loader/develop.svg?style=flat-square)](https://travis-ci.org/ga4gh/refget-loader)
[![Coverage Status](https://img.shields.io/coveralls/github/ga4gh/refget-loader/develop?style=flat-square)](https://coveralls.io/github/ga4gh/refget-loader?branch=develop)


# Refget Loader
Load reference sequences to Public Cloud Storage from a variety of data sources

## Overview

### Supported Cloud Environments
* AWS S3

### Supported Data Sources
* ENA Assemblies

## Getting Started

### Prerequisites

* Install the [AWS Command Line Interface](https://aws.amazon.com/cli/), and **configure** the CLI to run with an IAM user/profile that has write access to the S3 bucket of interest 
* Install the [ena-refget-processor](https://github.com/andrewyatz/ena-refget-processor) using the instructions provided, the scheduler will make use of its `load_expanded_con.pl` script

### Installation

Clone repo and install locally
```
git clone https://github.com/ga4gh/ena-refget-scheduler.git
cd ena-refget-scheduler
python setup.py install
```

Confirm the scheduler has been installed by issuing:
```
ena-refget-scheduler
```

### Usage

#### View / Modify Settings

#### View / Modify Upload Checkpoint

#### Schedule Upload Jobs
