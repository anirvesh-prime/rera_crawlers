import time
import unittest

import httpx

from core.crawler_base import download_response


class _FakeStreamResponse:
    def __init__(self, chunks: list[bytes], delay_between_chunks: float = 0.0):
        self.status_code = 200
        self.headers = {"Content-Type": "application/pdf"}
        self.request = httpx.Request("GET", "https://example.com/document.pdf")
        self.history = []
        self.extensions = {}
        self._chunks = chunks
        self._delay_between_chunks = delay_between_chunks
        self.closed = False

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self, chunk_size: int = 65536):
        for idx, chunk in enumerate(self._chunks):
            if idx and self._delay_between_chunks:
                time.sleep(self._delay_between_chunks)
            yield chunk

    def close(self) -> None:
        self.closed = True


class _FakeStreamContext:
    def __init__(self, response: _FakeStreamResponse):
        self._response = response

    def __enter__(self) -> _FakeStreamResponse:
        return self._response

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeClient:
    def __init__(self, response: _FakeStreamResponse):
        self._response = response

    def stream(self, *args, **kwargs):
        return _FakeStreamContext(self._response)


class DownloadResponseTests(unittest.TestCase):
    def test_returns_streamed_content_when_within_deadline(self):
        response = _FakeStreamResponse([b"abc", b"def"])
        client = _FakeClient(response)

        downloaded = download_response(
            "https://example.com/document.pdf",
            client=client,
            retries=1,
            total_timeout=1.0,
        )

        self.assertIsNotNone(downloaded)
        self.assertEqual(downloaded.content, b"abcdef")

    def test_returns_none_when_total_timeout_is_hit(self):
        response = _FakeStreamResponse([b"abc", b"def"], delay_between_chunks=0.05)
        client = _FakeClient(response)

        downloaded = download_response(
            "https://example.com/document.pdf",
            client=client,
            retries=1,
            total_timeout=0.01,
        )

        self.assertIsNone(downloaded)
        self.assertTrue(response.closed)


if __name__ == "__main__":
    unittest.main()
