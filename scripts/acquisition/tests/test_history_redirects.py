"""Fail closed on mismatched resources, hosts or credentials before allowing redirects."""

from pathlib import Path

import pytest

from scripts.acquisition.tests.test_capture import Opener, Response
from tests.support import check


def test_reviewed_redirect_is_exact_and_publisher_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import history_redirects, transport

    original = "https://example.org/dataset/example/resource/original/download/example.xlsx"
    target = "https://s3.amazonaws.com/example-public-bucket/resources/original/example.xlsx"
    monkeypatch.setattr(transport, "SIGNED_REDIRECT_ROUTES", {original: target})
    source = "https://example.org/dataset/example/resource/earlier/download/earlier.xlsx"
    destination = "https://s3.amazonaws.com/example-public-bucket/resources/earlier/earlier.xlsx"
    history_redirects.install_redirect(source, destination)
    check(transport.SIGNED_REDIRECT_ROUTES[source] == destination, "transport.SIGNED_REDIRECT_ROUTES[source] == destination")
    for bad_source, bad_target in [
        (source, destination + "?X-Amz-Signature=example"),
        (source.replace("example.org", "different.example"), destination),
        (source, destination.replace("/earlier/", "/other/")),
        (source, destination.replace("example-public-bucket", "different-bucket")),
    ]:
        with pytest.raises(ValueError):
            history_redirects.install_redirect(bad_source, bad_target)


class ReferenceOpener(Opener):
    """An opener whose signed reference already resolved to its publisher metadata."""

    def resolved_metadata(self, url: str) -> tuple[str, bool]:
        return ("https://example.org/reference.pdf", True)


# Matching public document MIME types are allowed; HTML or an unrelated document type still fails closed.
@pytest.mark.parametrize("media,body,complete", [("application/pdf", b"%PDF-1.4\nExample\n%%EOF", True), ("text/html", b"<html>Example error</html>", False)])
def test_signed_reference_response_keeps_type_and_byte_checks(tmp_path: Path, media: str, body: bytes, complete: bool) -> None:
    from email.message import Message

    from scripts.acquisition.transport import Limits, download

    response = Response(body)
    response.headers = Message()
    response.headers["Content-Type"] = media
    response.headers["Content-Length"] = str(len(body))
    opener = ReferenceOpener(response)
    result = download("https://example.org/reference.pdf", tmp_path, "reference.pdf", "pdf", "methodology", Limits(attempts=1), opener=opener)
    check(result.complete == complete, "result.complete == complete")
