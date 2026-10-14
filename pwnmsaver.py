#!/usr/bin/env python3

import argparse
import json
import os
import re
import signal
import sys
import tarfile
import gzip
import shutil
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

CFGFILE = "/usr/local/etc/wismon-cfg.json"
MAXFNAM = 85

cfg = {}
topics = []


def setcfg(key):
    with open(CFGFILE, "r") as fp:
        js = json.load(fp)

    if key not in js:
        raise RuntimeError(f"unresolvable {key}")

    u = urlparse(js[key])

    cfg["scheme"] = u.scheme
    cfg["host"] = u.hostname
    cfg["port"] = u.port
    cfg["username"] = u.username
    cfg["password"] = u.password

    topic = u.path.lstrip("/")
    if u.fragment:
        topic += "#" + u.fragment

    if topic:
        topics.append(topic)


class TarSaver:

    def __init__(self, fnpat):
        self.fnpat = fnpat
        self.fnam = None
        self.tar = None
        self.seq = 0

        self.clear()

    def mkfnam(self, ofs=0):
        now = datetime.now(timezone.utc) + timedelta(seconds=ofs)
        return now.strftime(self.fnpat)

    def clear(self):
        oldfile = self.mkfnam(-3600)

        if os.path.exists(oldfile):
            print(f"gzip -f {oldfile}", file=sys.stderr)
            self.gzip_file(oldfile)

    @staticmethod
    def gzip_file(fname):

        gzname = fname + ".gz"

        with open(fname, "rb") as src:
            with gzip.open(gzname, "wb") as dst:
                shutil.copyfileobj(src, dst)

        os.remove(fname)

    def save(self, topic, payload):

        fnam = self.mkfnam()

        if self.fnam != fnam:

            if self.tar:
                self.tar.close()

                print(f"gzip -f {self.fnam}", file=sys.stderr)
                self.gzip_file(self.fnam)

            self.fnam = fnam

            self.tar = tarfile.open(
                self.fnam,
                mode="a"
            )

            print(f"open {self.fnam}", file=sys.stderr)

            self.seq = 0

        now = datetime.now(timezone.utc)

        tnam = re.sub(
            r'^\w+/a/wis2/',
            '',
            topic
        )

        while len(tnam) > MAXFNAM:
            newname = re.sub(
                r'(\/\w)[-\w]+',
                r'\1',
                tnam,
                count=1
            )

            if newname == tnam:
                break

            tnam = newname

        if len(tnam) > MAXFNAM:
            tnam = tnam[:MAXFNAM]

        tnam = tnam.replace("/", "_")

        seqname = f"{self.seq:04X}"

        mnam = (
            f"{now.strftime('%d%H')}"
            f"{seqname}-"
            f"{tnam}.json"
        )

        if verbose:
            print(
                f"save {topic} {mnam}",
                file=sys.stderr
            )

        data = payload.encode("utf-8") \
            if isinstance(payload, str) \
            else payload

        info = tarfile.TarInfo(mnam)
        info.size = len(data)
        info.mtime = now.timestamp()

        import io

        self.tar.addfile(
            info,
            io.BytesIO(data)
        )

        self.seq += 1

    def close(self):

        if self.tar:
            self.tar.close()

        self.tar = None
        self.fnam = None


def build_client():

    scheme = cfg.get("scheme", "mqtts")

    transport = "tcp"
    tls = False

    if scheme == "mqtt":
        transport = "tcp"

    elif scheme == "mqtts":
        transport = "tcp"
        tls = True

    elif scheme == "ws":
        transport = "websockets"

    elif scheme == "wss":
        transport = "websockets"
        tls = True

    client = mqtt.Client(
        transport=transport
    )

    if cfg.get("username"):
        client.username_pw_set(
            cfg["username"],
            cfg.get("password")
        )

    if tls:
        client.tls_set()

    if transport == "websockets":

        path = cfg.get("ws_path", "/")

        client.ws_set_options(
            path=path
        )

    client.reconnect_delay_set(
        min_delay=5,
        max_delay=300
    )

    return client


def on_connect(client, userdata, flags, rc, properties=None):

    print(
        f"connected {cfg['host']}:{cfg['port']}",
        file=sys.stderr
    )

    for topic in topics:
        client.subscribe(topic)

        print(
            f"subscribed {topic}",
            file=sys.stderr
        )


def on_message(client, userdata, msg):

    try:
        saver.save(
            msg.topic,
            msg.payload
        )

    except Exception as e:
        print(
            f"save error: {e}",
            file=sys.stderr
        )


def sig_handler(signum, frame):

    saver.close()

    print(
        f"signal {signum} - quitting",
        file=sys.stderr
    )

    sys.exit(16)


parser = argparse.ArgumentParser()

parser.add_argument(
    "-h1",
    "--host",
    dest="host"
)

parser.add_argument(
    "-p",
    "--port",
    type=int,
    dest="port"
)

parser.add_argument(
    "-u",
    "--user",
    dest="username"
)

parser.add_argument(
    "-s",
    "--pass",
    dest="password"
)

parser.add_argument(
    "-o",
    "--out",
    required=True,
    dest="fnpat"
)

parser.add_argument(
    "-v",
    "--verbose",
    action="store_true"
)

parser.add_argument(
    "args",
    nargs="*"
)

args = parser.parse_args()

verbose = args.verbose

for a in args.args:

    if "host" not in cfg:
        setcfg(a)

    else:
        topics.append(a)

if args.host:
    cfg["host"] = args.host

if args.port:
    cfg["port"] = args.port

if args.username:
    cfg["username"] = args.username

if args.password:
    cfg["password"] = args.password

if not cfg.get("host"):
    raise RuntimeError("host not specified")

if not topics:
    raise RuntimeError("no topic specified")

scheme = cfg.get("scheme", "mqtts")

if scheme == "mqtt":
    cfg.setdefault("port", 1883)

elif scheme == "mqtts":
    cfg.setdefault("port", 8883)

elif scheme == "ws":
    cfg.setdefault("port", 80)

elif scheme == "wss":
    cfg.setdefault("port", 443)

u = None

try:
    with open(CFGFILE, "r") as fp:
        js = json.load(fp)

    for val in js.values():

        uu = urlparse(val)

        if uu.hostname == cfg["host"]:
            u = uu
            break

except Exception:
    pass

if u and u.path:
    cfg["ws_path"] = u.path

saver = TarSaver(args.fnpat)

signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)

client = build_client()

client.on_connect = on_connect
client.on_message = on_message

while True:

    try:

        client.connect(
            cfg["host"],
            cfg["port"],
            60
        )

        client.loop_forever()

    except KeyboardInterrupt:
        sig_handler(signal.SIGINT, None)

    except Exception as e:

        print(
            f"connection error: {e}",
            file=sys.stderr
        )
