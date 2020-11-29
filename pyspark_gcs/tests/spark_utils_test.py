import tempfile
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from pyspark_gcs.spark_utils import _get_spark_jars_path, get_gcs_enabled_config


def test_get_spark_jars_path__env_var_fail(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("SPARK_HOME", "/ala/ma/kota")
    with pytest.raises(
        ValueError,
        match="Can't find Spark jars, my best guess was: `/ala/ma/kota/jars`",
    ):
        _get_spark_jars_path()


def test_get_spark_jars_path__env_var_happy(monkeypatch: MonkeyPatch) -> None:
    d = Path(tempfile.mkdtemp())
    monkeypatch.setenv("SPARK_HOME", d.as_posix())
    d.joinpath("jars").mkdir()
    assert _get_spark_jars_path().as_posix() == f"{d.as_posix()}/jars"


def test_get_spark_jars_path__pyspark_module() -> None:
    import pyspark

    assert Path(pyspark.__file__).parent.as_posix() in _get_spark_jars_path().as_posix()


def test_config_connector_already_in_jars(monkeypatch: MonkeyPatch) -> None:
    d = Path(tempfile.mkdtemp())
    jars = d.joinpath("jars")
    monkeypatch.setenv("SPARK_HOME", d.as_posix())
    jars.mkdir()
    jars.joinpath("gcs-connector-hadoop2-2.1.1.jar").touch()
    c = get_gcs_enabled_config("foobar")
    assert c.contains("spark.hadoop.fs.gs.impl")
    assert c.get("spark.hadoop.fs.gs.project.id") == "foobar"
    assert not c.contains("spark.jars")


def test_config_use_our_connector(monkeypatch: MonkeyPatch) -> None:
    d = Path(tempfile.mkdtemp())
    jars = d.joinpath("jars")
    monkeypatch.setenv("SPARK_HOME", d.as_posix())
    jars.mkdir()
    c = get_gcs_enabled_config("foobar")
    assert c.contains("spark.hadoop.fs.gs.impl")
    assert c.get("spark.hadoop.fs.gs.project.id") == "foobar"
    assert "gcs-connector-hadoop3-2.1.6-shaded.jar" in c.get("spark.jars")
    jars.joinpath("hadoop-common-2-blah.jar").touch()
    c = get_gcs_enabled_config("foobar")
    assert c.contains("spark.hadoop.fs.gs.impl")
    assert c.get("spark.hadoop.fs.gs.project.id") == "foobar"
    assert "gcs-connector-hadoop2-2.1.6-shaded.jar" in c.get("spark.jars")
