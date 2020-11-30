# -*- coding: utf-8 -*-
# Copyright (c) 2020 Rafal Wojdyla
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import logging
import os
from pathlib import Path
from typing import Optional, Union

from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _get_spark_jars_path() -> Path:
    if "SPARK_HOME" in os.environ:
        logger.debug(
            f"Using SPARK_HOME {os.environ['SPARK_HOME']} to find jars directory"
        )
        jars = Path(os.environ["SPARK_HOME"]).joinpath("jars")
    else:
        logger.debug("Using pyspark package to find jars directory")
        import pyspark

        jars = Path(pyspark.__file__).parent.joinpath("jars")

    if not jars.exists():
        raise ValueError(
            f"Can't find Spark jars, my best guess was: `{jars.as_posix()}`"
            f", you can use `SPARK_HOME` to help out!"
        )
    return jars


def _connector_already_installed(jars: Path) -> bool:
    if len(list(jars.glob("*gcs-connector*"))) > 0:
        return True
    return False


def _find_major_version_of_hadoop(jars: Path) -> int:
    return 2 if len(list(jars.glob("hadoop-common-2*"))) else 3


def get_gcs_enabled_config(
    project: Optional[str] = None,
    jars_dir: Optional[Path] = None,
    conf: Optional[SparkConf] = None,
    service_account_keyfile_path: Optional[Path] = None,
) -> SparkConf:
    """Returns GCS enabled SparkConf object, which you can use to create Session/Context"""
    jconf = conf._jconf if conf else None
    conf = SparkConf(_jconf=jconf).set(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    if project:
        conf.set("spark.hadoop.fs.gs.project.id", project)

    if service_account_keyfile_path:
        conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "true").set(
            "spark.hadoop.fs.gs.auth.service.account.json.keyfile",
            service_account_keyfile_path.as_posix(),
        )

    jars = jars_dir or _get_spark_jars_path()
    if not _connector_already_installed(jars):
        hadoop_major = _find_major_version_of_hadoop(jars)
        # our setup.cfg says that this package should not be installed as zip
        # so we should be able to operate on files like FS to find the
        # gcs jar file:
        if hadoop_major == 2:
            logger.debug("Inferred Hadoop 2, using hadoop2 GCS connector jar")
            gcs_connector_jar = Path(__file__).parent.joinpath(
                "jars", "gcs-connector-hadoop2-2.1.6-shaded.jar"
            )
        else:
            logger.debug("Inferred Hadoop 3, using hadoop3 GCS connector jar")
            gcs_connector_jar = Path(__file__).parent.joinpath(
                "jars", "gcs-connector-hadoop3-2.1.6-shaded.jar"
            )
        assert gcs_connector_jar.exists(), (
            "There is something wrong with the pyspark_gcs installation, "
            f"can't find {gcs_connector_jar.as_posix()}"
        )
        conf.set("spark.jars", gcs_connector_jar.as_posix())

    return conf


def get_spark_session(
    service_account_keyfile_path: Union[str, Path, None] = None,
    project: Optional[str] = None,
    conf: Optional[SparkConf] = None,
) -> SparkSession:
    """
    Returns GCS enabled SparkSession. You may specify `service_account_keyfile_path`,
    `project`, and/or extra Spark config in the `conf` parameter.
    """
    sa_keyfile = (
        Path(service_account_keyfile_path) if service_account_keyfile_path else None
    )
    if sa_keyfile and not sa_keyfile.exists():
        raise ValueError(
            f"Service Account keyfile {service_account_keyfile_path} does not exist"
        )
    return SparkSession.builder.config(
        conf=get_gcs_enabled_config(project, conf=conf),
    ).getOrCreate()
