# Bubble

A simple HTTP proxy that redirects incoming traffic to a map of given upstreams.

The user can map individual client destination ports to specific upstreams. The actual client destination is discarded (except for the port that is used to select the upstream).

Optionally "PROXY protocol" can be added towards upstreams.

This proxy also serves a customizable PAC file provided in config, at location "<this proxy address>:<proxy port>/proxy.pac".

The proxy is written in python.

## Usage

    python bubble.py <path to ini config file>

A sample config file and Dockerfile is provided.
