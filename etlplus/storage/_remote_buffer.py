"""
:mod:`etlplus.storage._remote_buffer` module.

Shared in-memory file-like buffers for remote object storage backends.
"""

from __future__ import annotations

from collections.abc import Callable
from io import BytesIO
from io import TextIOWrapper
from typing import Any
from typing import Literal

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'open_remote_buffer',
    'parse_remote_open_mode',
]


# SECTION: TYPE ALIASES ===================================================== #


type RemoteOpenKind = Literal['read', 'write']
type UploadCallback = Callable[[bytes], None]


# SECTION: INTERNAL CLASSES ================================================= #


class _UploadOnCloseBytesIO(BytesIO):
    """Bytes buffer that uploads its payload when closed."""

    def __init__(
        self,
        uploader: UploadCallback,
    ) -> None:
        super().__init__()
        self._uploader = uploader
        self._uploaded = False

    def close(self) -> None:
        """Upload buffered bytes exactly once before closing."""
        if not self.closed and not self._uploaded:
            self._uploader(self.getvalue())
            self._uploaded = True
        super().close()


# SECTION: FUNCTIONS ======================================================== #


def open_remote_buffer(
    *,
    kind: RemoteOpenKind,
    text_mode: bool,
    payload: bytes | None = None,
    uploader: UploadCallback | None = None,
    encoding: str = 'utf-8',
    errors: str | None = None,
    newline: str | None = None,
) -> Any:
    """
    Build an in-memory file-like object for remote storage payloads.

    Parameters
    ----------
    kind : RemoteOpenKind
        Read or write mode.
    text_mode : bool
        Whether to expose a text stream instead of raw bytes.
    payload : bytes | None, optional
        Initial payload for read buffers.
    uploader : UploadCallback | None, optional
        Callback invoked with final bytes when a write buffer closes.
    encoding : str, optional
        Text encoding for text-mode streams.
    errors : str | None, optional
        Text decoding or encoding error mode.
    newline : str | None, optional
        Newline handling forwarded to :class:`TextIOWrapper`.

    Returns
    -------
    Any
        A readable or writable in-memory file-like object.

    Raises
    ------
    ValueError
        If required payload or uploader inputs are missing.
    """
    if kind == 'read':
        if payload is None:
            raise ValueError('payload is required for remote read buffers')
        raw_buffer = BytesIO(payload)
        if not text_mode:
            return raw_buffer
        return TextIOWrapper(
            raw_buffer,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    if uploader is None:
        raise ValueError('uploader is required for remote write buffers')

    raw_buffer = _UploadOnCloseBytesIO(uploader)
    if not text_mode:
        return raw_buffer
    return TextIOWrapper(
        raw_buffer,
        encoding=encoding,
        errors=errors,
        newline=newline,
    )


def parse_remote_open_mode(
    mode: str,
) -> tuple[RemoteOpenKind, bool]:
    """
    Normalize supported remote-storage open modes.

    Parameters
    ----------
    mode : str
        Requested file mode.

    Returns
    -------
    tuple[RemoteOpenKind, bool]
        ``(kind, text_mode)`` where ``kind`` is ``'read'`` or ``'write'`` and
        ``text_mode`` indicates text-vs-binary handling.

    Raises
    ------
    ValueError
        If *mode* is not one of the supported remote-storage modes.
    """
    supported_modes = {'r', 'rb', 'rt', 'w', 'wb', 'wt'}
    if mode not in supported_modes:
        raise ValueError(
            'Remote storage backends support only r/rb/rt/w/wb/wt modes',
        )
    kind: RemoteOpenKind = 'write' if mode.startswith('w') else 'read'
    return kind, 'b' not in mode
