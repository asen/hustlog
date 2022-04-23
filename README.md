# Hustlog

Disclaimer: This is my first non-trivial Rust project and I am using it to learn the language.

The goal of the project is to become a "swiss army knife" for logs, maybe even "ffmpeg for logs"

For now its mostly an attempt to create a CLI for the great [rust GROK library](https://github.com/daschl/grok)

Example usage:

    cargo build
    ./target/debug/hustlog --help # not everything is implemented
    ./target/debug/hustlog --grok-list-default-patterns # to see the built-in patterns
    ./target/debug/hustlog -i /var/log/system.log -p SYSLOGLINE -s "+timestamp:ts:%b %e %H:%M:%S" -s +message -m

