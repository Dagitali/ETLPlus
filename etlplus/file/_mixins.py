"""
:mod:`etlplus.file._mixins` module.

Reusable mixins extracted from file handler ABCs.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ._io import ScientificDatasetOption
from ._io import coerce_record_payload as _coerce_record_payload
from ._io import normalize_records
from ._io import require_dict_payload as _require_dict_payload
from ._io import stringify_value
from ._io import write_text

if TYPE_CHECKING:
    from .base import ReadOptions
    from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'SemiStructuredPayloadMixin',
    'SingleDatasetValidation',
    'TemplateTextIOMixin',
    'RegexTemplateRenderMixin',
]


# SECTION: CLASSES ========================================================== #


class SemiStructuredPayloadMixin:
    """
    Shared payload coercion helpers for semi-structured text handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Instance Methods -- #

    def coerce_dict_root_payload(
        self,
        payload: object,
        *,
        error_message: str | None = None,
    ) -> JSONDict:
        """
        Coerce ``payload`` to a dictionary or raise ``TypeError``.
        """
        if isinstance(payload, dict):
            return cast(JSONDict, payload)
        if error_message is None:
            error_message = f'{self.format_name} root must be a dict'
        raise TypeError(error_message)

    def coerce_record_payload(
        self,
        payload: Any,
    ) -> JSONData:
        """
        Coerce ``payload`` into object-or-object-list record form.
        """
        return _coerce_record_payload(payload, format_name=self.format_name)

    def require_dict_payload(
        self,
        data: JSONData,
    ) -> JSONDict:
        """
        Validate and return one dictionary payload.
        """
        return _require_dict_payload(data, format_name=self.format_name)


class SingleDatasetValidation(ScientificDatasetOption):
    """
    Shared helpers for single-dataset scientific handler variants.
    """

    dataset_key: ClassVar[str]
    format_name: str

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return the single supported dataset key.
        """
        _ = path
        return [self.dataset_key]

    def resolve_single_dataset(
        self,
        dataset: str | None = None,
        *,
        options: ReadOptions | WriteOptions | None = None,
    ) -> str | None:
        """
        Resolve and validate single-dataset selection.
        """
        resolved = self.resolve_dataset(dataset, options=options)
        self.validate_single_dataset_key(resolved)
        return resolved

    def validate_single_dataset_key(
        self,
        dataset: str | None,
    ) -> None:
        """
        Validate that *dataset* is either omitted or the default key.
        """
        if dataset is None or dataset == self.dataset_key:
            return
        raise ValueError(
            f'{self.format_name} supports only dataset key '
            f'{self.dataset_key!r}',
        )


class TemplateTextIOMixin:
    """
    Shared template-file read/write implementation.
    """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return a single template row from *path*.

        Parameters
        ----------
        path : Path
            Path to read from.
        options : ReadOptions | None, optional
            Read options, which may include encoding overrides. Defaults to
            ``None``.

        Returns
        -------
        JSONList
            List containing one dictionary with the template key and text
            value.
        """
        template_handler = cast(Any, self)
        return [
            {
                template_handler.template_key: path.read_text(
                    encoding=template_handler.encoding_from_options(options),
                ),
            },
        ]

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one template row to *path* and return row count.

        Parameters
        ----------
        path : Path
            Path to write to.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Write options, which may include encoding overrides. Defaults to
            ``None``.

        Returns
        -------
        int
            Number of rows written (0 or 1).

        Raises
        ------
        TypeError
             If *data* is not a one-item list of dictionaries with a string
                value for the template key.
        """
        template_handler = cast(Any, self)
        rows = normalize_records(data, template_handler.format_name)
        if not rows:
            return 0
        if len(rows) != 1:
            raise TypeError(
                f'{template_handler.format_name} payloads must contain '
                'exactly one object',
            )
        template_value = rows[0].get(template_handler.template_key)
        if not isinstance(template_value, str):
            raise TypeError(
                f'{template_handler.format_name} payloads must include a '
                f'"{template_handler.template_key}" string',
            )
        write_text(
            path,
            template_value,
            encoding=template_handler.encoding_from_options(options),
        )
        return 1


class RegexTemplateRenderMixin:
    """
    Shared regex-token template rendering implementation.
    """

    token_pattern: ClassVar[re.Pattern[str]]

    def template_key_from_match(
        self,
        match: re.Match[str],
    ) -> str | None:
        """
        Resolve one context key from a regex token match.
        """
        return cast(str | None, match.groupdict().get('key'))

    def render(
        self,
        template: str,
        context: JSONDict,
    ) -> str:
        """
        Render template text by replacing regex token matches with context
        values.
        """

        def _replace(match: re.Match[str]) -> str:
            key = self.template_key_from_match(match)
            return stringify_value(context.get(key)) if key is not None else ''

        return self.token_pattern.sub(_replace, template)
