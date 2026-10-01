"""Review exact historical resource redirects within the already trusted publisher bucket."""

import argparse
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from scripts.acquisition import transport
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import read_json


def install_redirect(source: str, target: str) -> None:
    """Register only an exact HTTPS resource redirect into the previously reviewed publisher bucket."""
    transport.safe_url(source)
    transport.safe_url(target)
    parsed = urllib.parse.urlsplit(target)
    if parsed.query:
        raise transport.CaptureError("Only unsigned destinations may be retained in a historical redirect review.")
    approved_source, approved_target = next(iter(transport.SIGNED_REDIRECT_ROUTES.items()))
    resource = re.fullmatch(r"/dataset/[^/]+/resource/([^/]+)/download/([^/]+)", urllib.parse.urlsplit(source).path)
    trusted_root = approved_target.split("/resources/")[0]
    expected = None if not resource else f"{trusted_root}/resources/{resource[1]}/{resource[2]}"
    if urllib.parse.urlsplit(source).hostname != urllib.parse.urlsplit(approved_source).hostname or target != expected:
        raise transport.CaptureError("Historical redirect must match the exact publisher, resource, filename and previously reviewed bucket.")
    transport.SIGNED_REDIRECT_ROUTES[source] = target


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Leave redirect responses unopened so exact destinations can be reviewed."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        """Permit only a reviewed HTTPS redirect without forwarding sensitive headers across hosts."""
        return None


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reviewed, failed = [], []
    for item in read_json(args.candidates)["candidates"]:
        request = transport.HttpsRequest(transport.safe_url(item["url"]), method="HEAD", headers={"User-Agent": "HistoricalSourceCapture/1.0"})
        response = None
        try:
            try:
                response = urllib.request.build_opener(NoRedirect()).open(request, timeout=30)
            except urllib.error.HTTPError as error:
                response = error
            if response.status not in {302, 303, 307, 308}:
                failed.append({"url": item["url"], "status": response.status})
                continue
            target = urllib.parse.urlsplit(response.headers.get("Location", ""))
            unsigned = urllib.parse.urlunsplit(target._replace(query="", netloc=target.netloc.removesuffix(":443")))
            install_redirect(item["url"], unsigned)
            reviewed.append(
                {
                    **item,
                    "reviewed_unsigned_redirect": unsigned,
                    "evidence": {
                        **item["evidence"],
                        "redirect_review": "Exact resource and filename in previously reviewed publisher bucket",
                        "redirect_observed_at": transport.utc_now(),
                    },
                }
            )
        except (ValueError, OSError):
            failed.append({"url": item["url"], "status": "redirect_not_verified"})
        finally:
            if response is not None:
                response.close()
    write_once(args.output, encoded_json({"candidates": reviewed, "unresolved_redirects": failed}))
    sys.stdout.write(str(f"Exact redirects verified: {len(reviewed)}; unresolved: {len(failed)}") + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
