ETL Operations
==============

The extract, validate, transform, load, and runner modules that power both the
CLI and Python API.

For most callers, :mod:`etlplus.ops` and :mod:`etlplus.ops.transform` are the
main entry points. The :mod:`etlplus.ops.transformations` package exposes the
step-level helpers that power :func:`etlplus.ops.transform.transform` and are
also available to custom runners that want to reuse one transformation family
without invoking the full orchestrator.

The transform orchestrator normalizes pipeline keys, applies steps in the fixed
order ``aggregate``, ``filter``, ``map``, ``select``, ``sort``, and returns a
single merged mapping when aggregate steps are present.

.. autosummary::
   :toctree: generated

   etlplus.ops.extract
   etlplus.ops.load
   etlplus.ops.run
   etlplus.ops.transform
   etlplus.ops.transformations
   etlplus.ops.transformations.aggregate
   etlplus.ops.transformations.filter
   etlplus.ops.transformations.map
   etlplus.ops.transformations.select
   etlplus.ops.transformations.sort
   etlplus.ops.validate
