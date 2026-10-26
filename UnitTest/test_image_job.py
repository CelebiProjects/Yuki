"""Tests for reusable algorithm image-job behavior."""

from CelebiChrono.utils import metadata
from Yuki.kernel.jobs.image import ImageJob


def test_lb_run_algorithm_uses_base_backend_environment(tmp_path):
    yaml_file = metadata.YamlFile(str(tmp_path / "celebi.yaml"))
    yaml_file.write_variable("environment", "lb-run")
    image = object.__new__(ImageJob)
    image.yaml_file = yaml_file

    assert image.environment() == image.default_environment()
