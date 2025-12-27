"""
:mod:`tests.unit.config.test_u_jobs` module.

Unit tests for :mod:`etlplus.config.jobs`.

Covers dataclass parsing, from_obj methods, and edge cases.
"""

import importlib

import pytest

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


jobs = importlib.import_module('etlplus.config.jobs')


# SECTION: TESTS ============================================================ #


# -- ExtractRef -- #


def test_extractref_from_obj_valid():
    """Test valid dict input yields expected :class:`ExtractRef` instance."""
    obj = {'source': 'my_source', 'options': {'foo': 1}}
    ref = jobs.ExtractRef.from_obj(obj)
    assert ref is not None
    assert ref.source == 'my_source'
    assert ref.options == {'foo': 1}


def test_extractref_from_obj_invalid():
    """Test invalid dict input yields None for :class:`ExtractRef`."""
    assert jobs.ExtractRef.from_obj(None) is None
    assert jobs.ExtractRef.from_obj({'source': 123}) is None


# -- JobConfig -- #


def test_jobconfig_from_obj_valid():
    """Test valid dict input yields expected :class:`JobConfig` instance."""
    obj = {
        'name': 'job1',
        'description': 'desc',
        'extract': {'source': 'src'},
        'validate': {'ruleset': 'rs'},
        'transform': {'pipeline': 'p'},
        'load': {'target': 't'},
    }
    cfg = jobs.JobConfig.from_obj(obj)
    assert cfg is not None
    assert cfg.name == 'job1'
    assert cfg.description == 'desc'
    assert cfg.extract is not None
    assert cfg.validate is not None
    assert cfg.transform is not None
    assert cfg.load is not None


def test_jobconfig_from_obj_invalid():
    """
    Test invalid dict input yields None or partial :class:`JobConfig` instance.
    """
    assert jobs.JobConfig.from_obj(None) is None
    assert jobs.JobConfig.from_obj({'name': 123}) is None
    cfg = jobs.JobConfig.from_obj({'name': 'x', 'description': 5})
    assert cfg is not None
    assert cfg.name == 'x'
    assert cfg.description == '5'


# -- LoadRef -- #


def test_loadref_from_obj_valid():
    """Test valid dict input yields expected :class:`LoadRef` instance."""
    obj = {'target': 'my_target', 'overrides': {'foo': 2}}
    ref = jobs.LoadRef.from_obj(obj)
    assert ref is not None
    assert ref.target == 'my_target'
    assert ref.overrides == {'foo': 2}


def test_loadref_from_obj_invalid():
    """Test invalid dict input yields None for :class:`LoadRef`."""
    assert jobs.LoadRef.from_obj({'target': 123}) is None


# -- TransformRef -- #


def test_transformref_from_obj_valid():
    """Test valid dict input yields expected :class:`TransformRef` instance."""
    obj = {'pipeline': 'my_pipeline'}
    ref = jobs.TransformRef.from_obj(obj)
    assert ref is not None
    assert ref.pipeline == 'my_pipeline'


def test_transformref_from_obj_invalid():
    """Test invalid dict input yields None for :class:`TransformRef`."""
    assert jobs.TransformRef.from_obj({'pipeline': 123}) is None


# -- ValidationRef -- #


def test_validationref_from_obj_valid():
    """
    Test valid dict input yields expected :class:`ValidationRef` instance.
    """
    obj = {'ruleset': 'rs', 'severity': 'warn', 'phase': 'both'}
    ref = jobs.ValidationRef.from_obj(obj)
    assert ref is not None
    assert ref.ruleset == 'rs'
    assert ref.severity == 'warn'
    assert ref.phase == 'both'


def test_validationref_from_obj_invalid():
    """
    Test invalid dict input yields None for :class:`ValidationRef`.
    """
    assert jobs.ValidationRef.from_obj(None) is None
    assert jobs.ValidationRef.from_obj({'ruleset': 123}) is None
