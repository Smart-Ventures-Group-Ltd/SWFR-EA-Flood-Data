"""One-off diagnostic: listen on the TAK stream and print how custom-icon markers are encoded.

Prints only the <detail> of markers that carry a <usericon>, with positions, device ids and parent
callsigns blanked (the repo's Actions logs are public). Used to compare a marker ATAK makes itself
with the feed's gauge markers.
"""
import os
import re
import socket
import ssl
import sys
import time

import tak_direct as td

LISTEN_S = int(os.environ.get("LISTEN_S", "360"))


def redact(ev):
    ev = re.sub(r'<point [^>]*/>', '<point …/>', ev)
    ev = re.sub(r'(lat|lon|hae|ce|le)="[^"]*"', r'\1="…"', ev)
    ev = re.sub(r'(<link [^>]*?)uid="[^"]*"', r'\1uid="…"', ev)
    ev = re.sub(r'parent_callsign="[^"]*"', 'parent_callsign="…"', ev)
    ev = re.sub(r'<takv [^>]*/>', '<takv …/>', ev)
    ev = re.sub(r'<remarks>.*?</remarks>', '<remarks>…</remarks>', ev, flags=re.S)
    return ev


def main():
    client = td.make_client()
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(client.cert_file, client.key_file)
    ctx.verify_flags &= ~getattr(ssl, "VERIFY_X509_STRICT", 0)
    if os.path.getsize(client.ca_file):
        ctx.load_verify_locations(cafile=client.ca_file)
    seen_feed, seen = False, set()
    try:
        with socket.create_connection((td.HOST, td.COT_PORT), timeout=30) as raw:
            with ctx.wrap_socket(raw, server_hostname=td.HOST) as s:
                s.sendall(td.self_pli().encode())
                s.settimeout(5)
                print(f"Listening for {LISTEN_S} s. Broadcast the test marker now.", flush=True)
                buf, end = "", time.monotonic() + LISTEN_S
                while time.monotonic() < end:
                    try:
                        data = s.recv(65536)
                    except socket.timeout:
                        continue
                    if not data:
                        print("server closed the connection")
                        break
                    buf += data.decode("utf-8", "replace")
                    while "</event>" in buf:
                        ev, buf = buf.split("</event>", 1)
                        ev = ev[ev.find("<event"):] + "</event>"
                        uid = (re.search(r'<event[^>]*\buid="([^"]+)"', ev) or [None, "?"])[1]
                        typ = (re.search(r'<event[^>]*\btype="([^"]+)"', ev) or [None, "?"])[1]
                        if "usericon" not in ev or uid in seen:
                            continue
                        feed = uid.startswith("SWFR-EA-")
                        if feed and seen_feed:
                            continue
                        seen.add(uid)
                        seen_feed = seen_feed or feed
                        print(f"\n=== {'FEED' if feed else 'OTHER'} marker type={typ}\n{redact(ev)}", flush=True)
    finally:
        client.close()
    print("\ndone")


if __name__ == "__main__":
    sys.exit(main())
