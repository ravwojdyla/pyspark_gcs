pyspark_gcs
===========

[![Build status](https://github.com/ravwojdyla/pyspark_gcs/workflows/Build/badge.svg?branch=master)](https://github.com/ravwojdyla/pyspark_gcs/actions?query=workflow%3A%22Build%22+branch%3Amaster)
[![GitHub license](https://img.shields.io/github/license/ravwojdyla/pyspark_gcs.svg)](./LICENSE)

## Raison d'être

pyspark package doesn't include [GCS FS support](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage).
pyspark user needs to manually configure/install GCS jars. This package adds GCS
batteries for pyspark. This is essentially a workaround for [SPARK-33605](https://issues.apache.org/jira/browse/SPARK-33605).

## Install

```
pip install pyspark_gcs
```

## Usage

```python
from pyspark_gcs import get_spark_session

spark = get_spark_session(service_account_keyfile_path="gcp_key.json")
```

`spark` is a pyspark session with GCS FS support. Because GCS connector doesn't yet
support Default Application Credentials [hadoop-connectors#59](https://github.com/GoogleCloudDataproc/hadoop-connectors/issues/59), as a user you need to provide `service_account_keyfile_path` or use
`GOOGLE_APPLICATION_CREDENTIALS` environment variable.
