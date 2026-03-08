"""
:mod:`tests.unit.file.test_u_file_binary_codec_handlers` module.

Unit tests for :mod:`etlplus.file._binary_codec_handlers`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import _binary_codec_handlers as mod
from etlplus.file.enums import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _CodecModuleStub:
    """Minimal codec-module stub with configurable encode/decode behavior."""

    def __init__(self) -> None:
        self.encode_calls: list[tuple[object, dict[str, object]]] = []
        self.decode_calls: list[tuple[bytes, dict[str, object]]] = []

    def encode(self, payload: object, **kwargs: object) -> bytes:
        """Record encode calls and return deterministic bytes."""
        self.encode_calls.append((payload, dict(kwargs)))
        return b'encoded'

    def decode(self, payload: bytes, **kwargs: object) -> object:
        """Record decode calls and return deterministic records."""
        self.decode_calls.append((payload, dict(kwargs)))
        return [{'id': 1}]


class _CodecHandler(mod.BinaryRecordCodecHandlerMixin):
    """Concrete codec handler stub for mixin-level unit tests."""

    format = FileFormat.CBOR
    codec_module_name = 'codec_stub'
    codec_format_name = 'CBOR'
    encode_method_name = 'encode'
    decode_method_name = 'decode'
    encode_kwargs = (('encode_opt', 1),)
    decode_kwargs = (('decode_opt', 2),)

    def __init__(self, codec_module: object) -> None:
        self._codec_module = codec_module

    def resolve_codec_module(self) -> object:
        """Return injected codec module."""
        return self._codec_module


# SECTION: TESTS ============================================================ #


class TestBinaryCodecHandlers:
    """Unit tests for shared binary codec handler mixin behavior."""

    def test_codec_method_raises_for_missing_callable(self) -> None:
        """
        Test that missing codec method errors include format/module metadata.
        """
        handler = _CodecHandler(_CodecModuleStub())
        with pytest.raises(
            AttributeError,
            match=(
                'CBOR codec module "codec_stub" must provide '
                'callable missing\\(\\)\\.'
            ),
        ):
            handler._codec_method(object(), 'missing')

    @pytest.mark.parametrize(
        ('data', 'expected_payload'),
        [
            ([{'id': 1}], [{'id': 1}]),
            ({'id': 1}, {'id': 1}),
        ],
        ids=['list_payload', 'dict_payload'],
    )
    def test_dumps_bytes_encodes_expected_payload_shape(
        self,
        data: object,
        expected_payload: object,
    ) -> None:
        """
        Test that the dumps path preserves list-vs-dict payload semantics.
        """
        codec = _CodecModuleStub()
        handler = _CodecHandler(codec)

        payload = handler.dumps_bytes(data)  # type: ignore[arg-type]

        assert payload == b'encoded'
        assert codec.encode_calls == [(expected_payload, {'encode_opt': 1})]

    def test_loads_bytes_decodes_and_coerces_records(self) -> None:
        """
        Test that the loads path forwards decode kwargs and coerces records.
        """
        codec = _CodecModuleStub()
        handler = _CodecHandler(codec)

        result = handler.loads_bytes(b'payload')

        assert result == [{'id': 1}]
        assert codec.decode_calls == [(b'payload', {'decode_opt': 2})]

    def test_read_and_write_file_paths_use_binary_base_flow(
        self,
        tmp_path: Path,
    ) -> None:
        """Test inherited binary path-level read/write behavior."""
        codec = _CodecModuleStub()
        handler = _CodecHandler(codec)
        path = tmp_path / 'payload.cbor'

        written = handler.write(path, [{'id': 1}])
        read_back = handler.read(path)

        assert written == 1
        assert read_back == [{'id': 1}]
        assert codec.encode_calls
        assert codec.decode_calls
