"""Speaks enough of the Postgres frontend protocol to prove the brokered path.

It runs inside a sandbox, where a stock template has python3 and no psql. It
connects to the brokered listener with a user and password that are
placeholders, runs one query and prints the first row, so whatever account
that row reports is the one the broker authenticated with rather than the one
written here.

Usage: postgres_broker_probe.py HOST PORT USER PASSWORD DATABASE SQL
Prints the row, or `ERR:<sqlstate>:<message>` and exits non-zero.
"""
import socket, struct, sys

host, port, user, password, database, sql = sys.argv[1:7]
# The password is never sent: the handler answers the guest's startup with
# AuthenticationOk once it has authenticated upstream itself. It is taken as an
# argument so a caller can prove that changing it changes nothing.
_ = password


def msg(tag, body):
    return tag + struct.pack("!I", len(body) + 4) + body


def startup(params):
    body = struct.pack("!I", 196608)
    for key, value in params:
        body += key.encode() + b"\0" + value.encode() + b"\0"
    body += b"\0"
    return struct.pack("!I", len(body) + 4) + body


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise SystemExit("ERR:upstream closed the connection")
        buf += chunk
    return buf


def read_message(sock):
    head = read_exact(sock, 5)
    length = struct.unpack("!I", head[1:5])[0]
    return head[0:1], read_exact(sock, length - 4)


sock = socket.create_connection((host, int(port)), timeout=15)
sock.sendall(startup([("user", user), ("database", database), ("application_name", "probe")]))

rows = []
while True:
    tag, body = read_message(sock)
    if tag == b"E":
        fields = {f[0:1]: f[1:].decode("utf8", "replace") for f in body.split(b"\0") if f}
        print("ERR:%s:%s" % (fields.get(b"C", "?"), fields.get(b"M", "?")))
        raise SystemExit(1)
    if tag == b"R":
        kind = struct.unpack("!I", body[0:4])[0]
        if kind != 0:
            print("ERR:the broker asked the guest for authentication %d" % kind)
            raise SystemExit(1)
        continue
    if tag == b"Z":  # ReadyForQuery
        if rows:
            print(rows[0])
            raise SystemExit(0)
        sock.sendall(msg(b"Q", sql.encode() + b"\0"))
        continue
    if tag == b"D":  # DataRow
        count = struct.unpack("!H", body[0:2])[0]
        at = 2
        values = []
        for _ in range(count):
            size = struct.unpack("!i", body[at:at + 4])[0]
            at += 4
            if size < 0:
                values.append(None)
            else:
                values.append(body[at:at + size].decode("utf8", "replace"))
                at += size
        rows.append("|".join("" if v is None else v for v in values))
