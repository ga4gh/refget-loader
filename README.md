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
