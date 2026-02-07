"""
:mod:`etlplus.ops.enums` module.

Operation-specific enums and helpers.
"""

from __future__ import annotations

import operator as _op
from statistics import fmean

from ..enums import CoercibleStrEnum
from ..types import StrStrMap
from .types import AggregateFunc
from .types import OperatorFunc

# SECTION: EXPORTS ========================================================= #


__all__ = [
    # Enums
    'AggregateName',
    'OperatorName',
    'PipelineStep',
]


# SECTION: ENUMS ============================================================ #


class AggregateName(CoercibleStrEnum):
    """Supported aggregations with helpers."""

    # -- Constants -- #

    AVG = 'avg'
    COUNT = 'count'
    MAX = 'max'
    MIN = 'min'
    SUM = 'sum'

    # -- Class Methods -- #

    @property
    def func(self) -> AggregateFunc:
        """
        Get the aggregation function for this aggregation type.

        Returns
        -------
        AggregateFunc
            The aggregation function corresponding to this aggregation type.
        """
        if self is AggregateName.COUNT:
            return lambda xs, n: n
        if self is AggregateName.MAX:
            return lambda xs, n: max(xs) if xs else None
        if self is AggregateName.MIN:
            return lambda xs, n: min(xs) if xs else None
        if self is AggregateName.SUM:
            return lambda xs, n: sum(xs)

        # AVG
        return lambda xs, n: fmean(xs) if xs else 0.0


class OperatorName(CoercibleStrEnum):
    """Supported comparison operators with helpers."""

    # -- Constants -- #

    EQ = 'eq'
    NE = 'ne'
    GT = 'gt'
    GTE = 'gte'
    LT = 'lt'
    LTE = 'lte'
    IN = 'in'
    CONTAINS = 'contains'

    # -- Getters -- #

    @property
    def func(self) -> OperatorFunc:
        """
        Get the comparison function for this operator.

        Returns
        -------
        OperatorFunc
            The comparison function corresponding to this operator.
        """
        match self:
            case OperatorName.EQ:
                return _op.eq
            case OperatorName.NE:
                return _op.ne
            case OperatorName.GT:
                return _op.gt
            case OperatorName.GTE:
                return _op.ge
            case OperatorName.LT:
                return _op.lt
            case OperatorName.LTE:
                return _op.le
            case OperatorName.IN:
                return lambda a, b: a in b
            case OperatorName.CONTAINS:
                return lambda a, b: b in a

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            '==': 'eq',
            '=': 'eq',
            '!=': 'ne',
            '<>': 'ne',
            '>=': 'gte',
            '≥': 'gte',
            '<=': 'lte',
            '≤': 'lte',
            '>': 'gt',
            '<': 'lt',
        }


class PipelineStep(CoercibleStrEnum):
    """Pipeline step names as an enum for internal orchestration."""

    # -- Constants -- #

    FILTER = 'filter'
    MAP = 'map'
    SELECT = 'select'
    SORT = 'sort'
    AGGREGATE = 'aggregate'

    # -- Getters -- #

    @property
    def order(self) -> int:
        """
        Get the execution order of this pipeline step.

        Returns
        -------
        int
            The execution order of this pipeline step.
        """
        return _PIPELINE_ORDER_INDEX[self]


# SECTION: INTERNAL CONSTANTS =============================================== #

# Precomputed order index for PipelineStep; avoids recomputing on each access.
_PIPELINE_ORDER_INDEX: dict[PipelineStep, int] = {
    PipelineStep.FILTER: 0,
    PipelineStep.MAP: 1,
    PipelineStep.SELECT: 2,
    PipelineStep.SORT: 3,
    PipelineStep.AGGREGATE: 4,
}
