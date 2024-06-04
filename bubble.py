from __future__ import annotations

import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import asyncio
import logging
import signal
import ipaddress
from typing import Tuple, Coroutine, Dict, cast
from functools import partial
import socket
import pathlib
import configparser
import time
from pathlib import Path
from enum import Enum

from proxyprotocol.server import Address
from proxyprotocol.result import ProxyResultIPv4, ProxyResultIPv6
from proxyprotocol.v1 import ProxyProtocolV1
from proxyprotocol.v2 import ProxyProtocolV2


class PROXY_PROTOCOL(Enum):
    NONE = 1
    PP_V1 = 2
    PP_V2 = 3


LOG = logging.getLogger(__name__)
BUFFER_LEN = 1024
UPSTREAM_CONNECTION_TIMEOUT = 5  # seconds

# cached responses
UNMAPPED_UPSTREAM_RESPONSE = (
    b"HTTP/1.1 500 Upstream not available for this port\r\n\r\n"
)
UNAVAILABLE_UPSTREAM_RESPONSE = b"HTTP/1.1 500 Upstream isn't responding\r\n\r\n"
UPSTREAM_REFUSED_RESPONSE = b"HTTP/1.1 500 Upstream refused the connection\r\n\r\n"
UPSTREAM_HOSTNAME_UNREACHABLE_RESPONSE = (
    b"HTTP/1.1 500 Upstream hostname unreachable\r\n\r\n"
)
UPSTREAM_FAILURE_RESPONSE = b"HTTP/1.1 500 Generic upstream connection fail\r\n\r\n"
OK_RESPONSE = b"HTTP/1.1 200 Ok\r\n\r\n"


class Upstream:
    def __init__(self, config_upstream_string: str):
        """Every time downstream requests a connection to Upstream.destination_port, proxy the
        connection towards Upstream.address using PROXY_protocol defined in Upstream.pp.
        """
        try:
            destination_port_s, upstream_address_pp_s = config_upstream_string.split(
                "->"
            )
        except ValueError as err:
            raise ValueError(
                f"Invalid upstream format for '{config_upstream_string}': it should be <destination ports>-><upstream address>:<upstream port>"
            ) from err

        try:
            self.destination_port = int(destination_port_s)
        except ValueError as err:
            raise ValueError(
                f"Invalid upstream format for '{config_upstream_string}': port must be an int"
            ) from err

        try:
            upstream_address_s, pp_s = upstream_address_pp_s.split("+")
        except ValueError as err:
            upstream_address_s = upstream_address_pp_s
            self.pp = PROXY_PROTOCOL.NONE
        else:
            if pp_s == "pp1":
                self.pp = PROXY_PROTOCOL.PP_V1
            elif pp_s == "pp2":
                self.pp = PROXY_PROTOCOL.PP_V2
            else:
                raise ValueError(
                    f"Invalid upstream format for '{config_upstream_string}': PROXY_protocol must be added with '+pp1' or '+pp2'"
                )

        self.address = Address(upstream_address_s)
        if self.address.port is None:
            raise ValueError(
                f"Invalid upstream format for '{config_upstream_string}': upstream must have explicit port"
            )


async def main() -> int:
    """The main function: gather() all servers listed in config."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "config",
        metavar="<config file>",
        type=pathlib.Path,
        nargs=1,
        help="the location of the *.ini config file",
    )
    args = parser.parse_args()

    config_path = args.config[0]
    if not config_path.is_file():
        print(f"Unable to locate {config_path}")
        return 1

    config = configparser.ConfigParser()
    config.read(config_path)

    try:
        log_level_s = config.get("general", "log_level")
    except configparser.NoSectionError:
        print("Invalid config file: no [general] section")
        return 1
    except configparser.NoOptionError:
        print("Invalid config file: no 'log_level' option in [general] section")
        return 1

    try:
        log_level = {
            "error": logging.ERROR,
            "warn": logging.WARN,
            "info": logging.INFO,
            "debug": logging.DEBUG,
        }[log_level_s]
    except KeyError:
        print(
            "Invalid config file: 'log_level' option in [general] section must be 'error', 'warn', "
            "'info' or 'debug'"
        )
        return 1

    logging.basicConfig(level=log_level, format="%(asctime)-15s %(name)s %(message)s")

    try:
        inactivity_timeout_s = config.get("general", "inactivity_timeout")
        # configparser.NoSectionError eventually raised by previous option query
    except configparser.NoOptionError:
        print(
            "Invalid config file: no 'inactivity_timeout' option in [general] section"
        )
        return 1
    try:
        inactivity_timeout = float(inactivity_timeout_s)
    except ValueError:
        print("Invalid config file: the 'inactivity_timeout' must be an int or a float")
        return 1

    try:
        listening_port_s = config.get("general", "listening_port")
        # configparser.NoSectionError eventually raised by previous option query
    except configparser.NoOptionError:
        print("Invalid config file: no 'listening_port' option in [general] section")
        return 1
    try:
        listening_port = int(listening_port_s)
    except ValueError:
        print("Invalid config file: the 'listening_port' must be an int")
        return 1

    try:
        upstreams_s = [x.strip() for x in config.get("general", "upstreams").split(",")]
        # configparser.NoSectionError eventually raised by previous option query
    except configparser.NoOptionError:
        print("Invalid config file: no 'upstreams' option in [general] section")
        return 1

    upstreams: Dict[int, Upstream] = {}
    for upstream_s in upstreams_s:
        try:
            upstream = Upstream(upstream_s)
        except ValueError as err:
            print(err)
            return 1
        upstreams[upstream.destination_port] = upstream

    try:
        PAC_file_s = config.get("general", "PAC_file")
    except configparser.NoOptionError:
        print("Invalid config file: missing 'PAC_file' option in [general] section")
        return 1
    PAC_file = Path(PAC_file_s)
    if not PAC_file.exists():
        print(f"PAC file '{PAC_file_s}' not found")
        return 1

    with open(PAC_file, "rb") as file:
        pac_file_contents = file.read()

    loop = asyncio.get_event_loop()

    try:
        server = await make_server(
            listening_port,
            inactivity_timeout,
            upstreams,
            pac_file_contents,
        )
    except OSError as err:
        LOG.error("Unable to run tcp proxy at local port %s", listening_port)
        LOG.error(err.strerror)
    else:
        forever = asyncio.create_task(server.serve_forever())
        LOG.info(
            "Started serving tcp proxy at local port %s",
            listening_port,
        )
        try:
            loop.add_signal_handler(signal.SIGINT, forever.cancel)
            loop.add_signal_handler(signal.SIGTERM, forever.cancel)
        except NotImplementedError:
            # windows
            pass

    try:
        await asyncio.gather(forever)
    except asyncio.CancelledError:
        pass
    return 0


def make_server(
    listening_port: int,
    inactivity_timeout: float,
    upstreams: Dict[int, Upstream],
    pac_file_contents: bytes,
) -> Coroutine:
    """Return a server that needs to be awaited."""

    address = Address(f"0.0.0.0:{listening_port}")
    proxy_partial = partial(
        proxy,
        inactivity_timeout=inactivity_timeout,
        upstreams=upstreams,
        pac_file_contents=pac_file_contents,
    )
    return asyncio.start_server(proxy_partial, address.host, address.port)


async def read_client_request(
    reader: asyncio.StreamReader, timeout: InactivityTimeout
) -> int | None:
    """Read the client HTTP request. We support 2 types of requests: CONNECT and GET.

    If a CONNECT request is detected, return the port of the destination the client wants to connect
    to. The rest of the address is discarded.

    If a GET request is detected, we check if the PAC file is requested. If so, return None.

    All other requests raise a ValueError.
    """
    try:
        recv = await asyncio.wait_for(reader.read(BUFFER_LEN), timeout.remaining)
        timeout.awake()
    except asyncio.TimeoutError:
        raise ValueError("Timed out client request")

    LOG.debug("Received request %s", recv)

    """The recv is expected to be:
    CONNECT <host>:<port> HTTP/1.1
    Host: <host>:<port>
    Proxy-Connection: keep-alive
    User-Agent: <stuff here>
    <two blank lines>

    Or:
    'GET /proxy.pac HTTP/1.1
    Connection: Keep-Alive
    Accept: */*
    User-Agent: <stuff here>
    Host: <stuff here>
    <two blank lines>
    """
    if recv[-4:] != b"\r\n\r\n":
        raise ValueError("Client request incomplete")

    # first line always exists
    first_line = recv.splitlines()[0]

    try:
        mode, subject, http = first_line.split()
    except ValueError as err:
        raise ValueError(
            f"Expected first request line to be '<verb> <subject> <http version>', got '{first_line!r}' instead"
        ) from err

    if http != b"HTTP/1.1":
        raise ValueError(f"Expected 'HTTP/1.1' request, got '{http!r}' instead")

    if mode == b"CONNECT":
        try:
            _, port_b = subject.split(b":")
        except ValueError as err:
            raise ValueError(
                f"Expected destination 'host:port', got '{subject!r}' instead"
            ) from err

        try:
            port = int(port_b.decode("ascii", "ignore"))
        except ValueError as err:
            raise ValueError(f"Invalid port '{port_b!r}'") from err

        return port

    elif mode == b"GET":
        if subject != b"/proxy.pac":
            raise ValueError(
                f"Only '/proxy.pac' GET requests are supported, got '{subject!r}' instead"
            )

        return None

    else:
        raise ValueError(f"Expected 'CONNECT' or 'GET' request, got '{mode!r}' instead")


def serve_PAC(writer: asyncio.StreamWriter, pac_file_contents: bytes) -> None:
    writer.write(OK_RESPONSE)
    writer.write(pac_file_contents)


async def open_upstream_connection(
    upstream: Address, writer: asyncio.StreamWriter, timeout: InactivityTimeout
) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter] | Tuple[None, None]:
    try:
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(upstream.host, upstream.port),
            UPSTREAM_CONNECTION_TIMEOUT,
        )
    except asyncio.TimeoutError:
        LOG.error("Failed to connect upstream: timed out")
        writer.write(UNAVAILABLE_UPSTREAM_RESPONSE)
        return None, None
    except ConnectionRefusedError as err:
        LOG.error("Failed to connect upstream: connection refused")
        LOG.error(err.strerror)
        writer.write(UPSTREAM_REFUSED_RESPONSE)
        return None, None
    except socket.gaierror as err:
        LOG.error(
            "Failed to connect upstream: unable to reach hostname %s",
            upstream.host,
        )
        LOG.error(err.strerror)
        writer.write(UPSTREAM_HOSTNAME_UNREACHABLE_RESPONSE)
        return None, None
    except OSError as err:
        LOG.error(
            "Failed to connect upstream: probably trying to connect to an https server"
        )
        LOG.error(err.strerror)
        writer.write(UPSTREAM_FAILURE_RESPONSE)
        return None, None
    else:
        LOG.debug("Requested SUCCEEDED: proxy is about to begin")
        writer.write(OK_RESPONSE)
        timeout.awake()
        return upstream_reader, upstream_writer


async def proxy(
    downstream_reader: asyncio.StreamReader,
    downstream_writer: asyncio.StreamWriter,
    inactivity_timeout: float,
    upstreams: Dict[int, Upstream],
    pac_file_contents: bytes,
) -> None:
    """Handle the incoming connection."""
    open_writers: tuple[asyncio.StreamWriter, ...] = (
        downstream_writer,
    )  # used to close them later

    downstream_ip_s, downstream_port = downstream_writer.get_extra_info("peername")
    downstream_ip = ipaddress.ip_address(downstream_ip_s)
    cast(int, downstream_port)

    log_signature = f"{downstream_ip}:{downstream_port}"

    LOG.debug("[%s] Incoming connection detected", log_signature)

    """The idea here is to have a shared timeout among the pipes. Every time any  pipe receives some
    data, the timeout is 'reset' and waits more time on both pipes.
    """
    timeout = InactivityTimeout(inactivity_timeout)

    try:
        destination_port = await read_client_request(downstream_reader, timeout)
    except ValueError as err:
        LOG.error(err)
    else:
        if destination_port is None:
            # PAC file requested
            serve_PAC(downstream_writer, pac_file_contents)
            LOG.info("[%s] Served PAC file", log_signature)
        else:
            if destination_port not in upstreams:
                LOG.error(
                    "[%s] The requested port %s has no matching upstream",
                    log_signature,
                    destination_port,
                )
                downstream_writer.write(UNMAPPED_UPSTREAM_RESPONSE)
            else:
                upstream = upstreams[destination_port]
                upstream_reader, upstream_writer = await open_upstream_connection(
                    upstream.address, downstream_writer, timeout
                )

                if upstream_reader is not None and upstream_writer is not None:
                    LOG.info(
                        "[%s] Upstream selected and connected: %s (requested port %s by client)",
                        log_signature,
                        upstream,
                        destination_port,
                    )

                    open_writers += (upstream_writer,)

                    if upstream.pp != PROXY_PROTOCOL.NONE:
                        LOG.debug("[%s] Apply PROXY_protocol", log_signature)
                        upstream_ip_s, upstream_port = upstream_writer.get_extra_info(
                            "peername"
                        )
                        upstream_ip = ipaddress.ip_address(upstream_ip_s)
                        cast(int, upstream_port)

                        result: ProxyResultIPv4 | ProxyResultIPv6 | None
                        if isinstance(
                            downstream_ip, ipaddress.IPv4Address
                        ) and isinstance(upstream_ip, ipaddress.IPv4Address):
                            result = ProxyResultIPv4(
                                source=(downstream_ip, downstream_port),
                                dest=(upstream_ip, upstream_port),
                            )
                        elif isinstance(
                            downstream_ip, ipaddress.IPv6Address
                        ) and isinstance(upstream_ip, ipaddress.IPv6Address):
                            result = ProxyResultIPv6(
                                source=(downstream_ip, downstream_port),
                                dest=(upstream_ip, upstream_port),
                            )
                        else:
                            result = None

                        if result is None:
                            LOG.debug(
                                "[%s] PROXY_protocol failed: non-matching IP address types for upstream and downstream",
                                log_signature,
                            )
                        else:
                            pp: ProxyProtocolV1 | ProxyProtocolV2
                            if upstream.pp == PROXY_PROTOCOL.PP_V1:
                                pp = ProxyProtocolV1()
                            elif upstream.pp == PROXY_PROTOCOL.PP_V2:
                                pp = ProxyProtocolV2()
                            upstream_writer.write(pp.pack(result))

                    timeout.awake()
                    forward_pipe = pipe(downstream_reader, upstream_writer, timeout)
                    backward_pipe = pipe(upstream_reader, downstream_writer, timeout)
                    await asyncio.gather(backward_pipe, forward_pipe)

                    await asyncio.sleep(0.1)  # wait for writes to actually drain

    LOG.debug("[%s] Closing connection", log_signature)

    for open_writer in open_writers:
        await close_write_stream(open_writer)


async def close_write_stream(writer: asyncio.StreamWriter) -> None:
    """Gracefully close the writer stream."""
    if writer.can_write_eof():
        try:
            writer.write_eof()
        except OSError:  # Socket not connected
            pass  # connection is lost, but we don't really care

    writer.close()
    try:
        await writer.wait_closed()
    except (ConnectionAbortedError, BrokenPipeError):
        pass


async def pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    timeout: InactivityTimeout,
) -> None:
    remaining_seconds = timeout.remaining
    while remaining_seconds and not reader.at_eof():
        try:
            writer.write(
                await asyncio.wait_for(reader.read(BUFFER_LEN), remaining_seconds)
            )
            timeout.awake()
        except asyncio.TimeoutError:
            pass

        remaining_seconds = timeout.remaining


class InactivityTimeout:
    """
    This Object handles shared pipe timeout.
    """

    def __init__(self, timeout: float):
        """
        The remaining time until timeout is prolonged every time we 'awake()' this
        InactivityTimeout.

        E.g. if we have t = InactivityTimeout(5), t.remaining should be around 5 seconds.
        If, after 3 seconds we query t.remaining again, we should get around 2 seconds.
        If we call t.awake() and we query t.remaining, it should be back again at around 5 seconds.
        """
        self.last_awake = time.time()
        self.timeout = timeout

    def awake(self) -> None:
        self.last_awake = time.time()

    @property
    def remaining(self) -> float:  # >= 0
        now = time.time()
        remaining = self.last_awake + self.timeout - now
        return max(remaining, 0)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
