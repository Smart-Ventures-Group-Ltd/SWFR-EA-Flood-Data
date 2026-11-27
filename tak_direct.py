"""Send CoT straight to the GoTAK TAK server as a TAK client.

Why: GoTAK's broadcast_markers tool emits a trimmed marker (no <archive/>, a
<uid Droid> contact tag, a new uid every send). iTAK drops the custom icon from
those on restart. Markers in ATAK's own format, with a fixed uid that is updated
in place, keep their icon through an iTAK restart (tested 26 Nov 2026).

Credentials, from GitHub secrets (never stored in the repo):
  Preferred - a GoTAK device (EUD) certificate from its data package:
    TAK_CLIENT_P12_B64, TAK_CLIENT_P12_PASSWORD
    TAK_TRUST_P12_B64, TAK_TRUST_P12_PASSWORD   (optional truststore)
  Fallback - a GoTAK account, enrolled each run on port 8446:
    TAK_FEED_USER, TAK_FEED_PASSWORD
Key and certificate exist only in a private temp folder for the run and are deleted after.
"""
import base64
import datetime as dt
import json
import os
import re
import socket
import ssl
import tempfile
import time
import urllib.parse
import urllib.request
import zlib
from xml.sax.saxutils import escape, quoteattr

HOST = os.environ.get("TAK_HOST", "eud.gotak.dev")
ENROL_PORT = int(os.environ.get("TAK_ENROL_PORT", "8446"))
COT_PORT = int(os.environ.get("TAK_COT_PORT", "8089"))
CLIENT_UID = os.environ.get("TAK_CLIENT_UID", "SWFR-EA-FLOOD-FEED")
CALLSIGN = os.environ.get("TAK_CALLSIGN", "EA Flood Feed")


def _iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def _now():
    return dt.datetime.now(dt.timezone.utc)


def _pem(b64, kind="CERTIFICATE"):
    b64 = re.sub(r"-----[^-]+-----|\s", "", b64)
    lines = "\n".join(b64[i:i + 64] for i in range(0, len(b64), 64))
    return f"-----BEGIN {kind}-----\n{lines}\n-----END {kind}-----\n"


def _b64(value):
    return base64.b64decode(re.sub(r"\s", "", value))


class _Client:
    def _write(self, cert_pem, key_pem, ca_pems):
        self._dir = tempfile.mkdtemp(prefix="takfeed-")
        os.chmod(self._dir, 0o700)
        self.cert_file = os.path.join(self._dir, "client.pem")
        self.key_file = os.path.join(self._dir, "client.key")
        self.ca_file = os.path.join(self._dir, "ca.pem")
        with open(self.cert_file, "w") as fh:
            fh.write(cert_pem)
        with os.fdopen(os.open(self.key_file, os.O_WRONLY | os.O_CREAT, 0o600), "w") as fh:
            fh.write(key_pem)
        with open(self.ca_file, "w") as fh:
            fh.write("".join(ca_pems))

    def close(self):
        for f in (self.cert_file, self.key_file, self.ca_file):
            try:
                os.remove(f)
            except OSError:
                pass
        try:
            os.rmdir(self._dir)
        except OSError:
            pass

    def send(self, events, pause=0.02):
        """Open one mTLS stream, announce ourselves, write every event, then close."""
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.load_cert_chain(self.cert_file, self.key_file)
        # TAK server CAs are often older-style certificates; Python 3.13's extra strict profile rejects
        # them (e.g. no Authority Key Identifier). The chain and hostname are still fully verified.
        ctx.verify_flags &= ~getattr(ssl, "VERIFY_X509_STRICT", 0)
        if os.path.getsize(self.ca_file):
            ctx.load_verify_locations(cafile=self.ca_file)
        with socket.create_connection((HOST, COT_PORT), timeout=30) as raw:
            with ctx.wrap_socket(raw, server_hostname=HOST) as s:
                s.sendall(self_pli().encode())
                time.sleep(1.0)
                for ev in events:
                    s.sendall(ev.encode())
                    if pause:
                        time.sleep(pause)
                time.sleep(2.0)  # let the server drain before we hang up
                try:
                    s.unwrap()  # clean TLS close so nothing in flight is discarded
                except (OSError, ssl.SSLError):
                    pass
        return len(events)


class FromP12(_Client):
    """Client certificate from a GoTAK device (EUD) data package."""

    def __init__(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.serialization import pkcs12

        pw = os.environ.get("TAK_CLIENT_P12_PASSWORD", "")
        key, cert, extra = pkcs12.load_key_and_certificates(_b64(os.environ["TAK_CLIENT_P12_B64"]),
                                                            pw.encode() if pw else None)
        cas = list(extra or [])
        if os.environ.get("TAK_TRUST_P12_B64", "").strip():
            tpw = os.environ.get("TAK_TRUST_P12_PASSWORD") or pw
            raw = _b64(os.environ["TAK_TRUST_P12_B64"])
            p = pkcs12.load_pkcs12(raw, tpw.encode() if tpw else None)
            cas += ([p.cert.certificate] if p.cert else []) + [c.certificate for c in p.additional_certs]
        enc = serialization.Encoding.PEM
        self._write(cert.public_bytes(enc).decode(),
                    key.private_bytes(enc, serialization.PrivateFormat.TraditionalOpenSSL,
                                      serialization.NoEncryption()).decode(),
                    [c.public_bytes(enc).decode() for c in cas])


class Enrolled(_Client):
    """Standard TAK Server certificate enrolment with a GoTAK account (as ATAK/iTAK do it)."""

    def __init__(self, user, password):
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        ctx = ssl.create_default_context()
        base = f"https://{HOST}:{ENROL_PORT}/Marti/api/tls"

        req = urllib.request.Request(f"{base}/config", headers={"Authorization": auth})
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            cfg = r.read().decode()
        names = {}
        for tag in re.findall(r"<nameEntry[^>]*>", cfg):
            n, v = re.search(r'name="([^"]+)"', tag), re.search(r'value="([^"]*)"', tag)
            if n and v:
                names[n.group(1)] = v.group(1)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        attrs = [x509.NameAttribute(NameOID.COMMON_NAME, user)]
        if names.get("O"):
            attrs.append(x509.NameAttribute(NameOID.ORGANIZATION_NAME, names["O"]))
        if names.get("OU"):
            attrs.append(x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, names["OU"]))
        csr = x509.CertificateSigningRequestBuilder().subject_name(x509.Name(attrs)).sign(key, hashes.SHA256())
        csr_body = re.sub(r"-----[^-]+-----|\s", "", csr.public_bytes(serialization.Encoding.PEM).decode())

        q = urllib.parse.urlencode({"clientUid": CLIENT_UID, "version": "swfr-ea-feed-1"})
        req = urllib.request.Request(f"{base}/signClient/v2?{q}", data=csr_body.encode(), method="POST",
                                     headers={"Authorization": auth, "Content-Type": "text/plain",
                                              "Accept": "application/json"})
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            body = r.read().decode()
        try:
            data = json.loads(body)
            cert = data["signedCert"]
            cas = [v for k, v in sorted(data.items()) if k.startswith("ca")]
        except (ValueError, KeyError):
            cert = re.search(r"<signedCert>([^<]+)</signedCert>", body).group(1)
            cas = re.findall(r"<ca>([^<]+)</ca>", body)
        self._write(_pem(cert), key.private_bytes(serialization.Encoding.PEM,
                                                  serialization.PrivateFormat.TraditionalOpenSSL,
                                                  serialization.NoEncryption()).decode(),
                    [_pem(c) for c in cas])


def configured():
    return bool(os.environ.get("TAK_CLIENT_P12_B64", "").strip()
                or (os.environ.get("TAK_FEED_USER") and os.environ.get("TAK_FEED_PASSWORD")))


def make_client():
    if os.environ.get("TAK_CLIENT_P12_B64", "").strip():
        return FromP12()
    return Enrolled(os.environ["TAK_FEED_USER"], os.environ["TAK_FEED_PASSWORD"])


def self_pli():
    """A minimal fixed presence so the server registers the feed client."""
    t = _now()
    return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<event version="2.0" uid="{CLIENT_UID}" type="a-f-G-E-S" how="m-g" time="{_iso(t)}" '
            f'start="{_iso(t)}" stale="{_iso(t + dt.timedelta(minutes=20))}">'
            f'<point lat="52.192" lon="-2.22" hae="9999999.0" ce="9999999.0" le="9999999.0"/>'
            f'<detail><contact callsign={quoteattr(CALLSIGN)}/><__group name="Cyan" role="HQ"/>'
            f'<takv device="GitHub Actions" platform="SWFR EA feed" os="linux" version="1"/></detail></event>')


def marker(uid, name, lat, lon, iconset_path, remarks, stale_hours=24):
    """ATAK-format custom-icon marker (the format proven to survive an iTAK restart)."""
    t = _now()
    return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<event version="2.0" uid={quoteattr(uid)} type="a-u-G" how="h-g-i-g-o" time="{_iso(t)}" '
            f'start="{_iso(t)}" stale="{_iso(t + dt.timedelta(hours=stale_hours))}">'
            f'<point lat="{lat}" lon="{lon}" hae="9999999.0" ce="9999999.0" le="9999999.0"/>'
            f'<detail><status readiness="true"/><archive/>'
            f'<link uid="{CLIENT_UID}" production_time="{_iso(t)}" type="a-f-G-E-S" '
            f'parent_callsign={quoteattr(CALLSIGN)} relation="p-p"/>'
            f'<contact callsign={quoteattr(name)}/><remarks>{escape(remarks)}</remarks>'
            f'<archive/><color argb="-1"/><usericon iconsetpath={quoteattr(iconset_path)}/></detail></event>')


def delete(uid):
    """Standard TAK delete for a map item on every receiving device."""
    t = _now()
    return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<event version="2.0" uid="{CLIENT_UID}-del-{zlib.crc32(uid.encode())}" type="t-x-d-d" '
            f'how="h-g-i-g-o" time="{_iso(t)}" start="{_iso(t)}" stale="{_iso(t + dt.timedelta(minutes=5))}">'
            f'<point lat="0" lon="0" hae="9999999.0" ce="9999999.0" le="9999999.0"/>'
            f'<detail><link uid={quoteattr(uid)} relation="none" type="none"/><__forcedelete/></detail></event>')
