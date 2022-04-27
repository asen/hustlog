# Hustlog

Disclaimer: This is my first non-trivial Rust project and I am using it to learn the language.

The goal of the project is to become a "swiss army knife" for logs, maybe even "ffmpeg for logs"

What it has for now: 

- log parser based on regular expressions and the rust [grok](https://crates.io/crates/grok) crate
- (partially supported/WIP) SQL interface to access the parsed data
(query parsing is using the rust [sqlparser](https://crates.io/crates/sqlparser) crate)

Example usage:

    cargo build
    ./target/debug/hustlog --help # not everything is implemented
    ./target/debug/hustlog --grok-list-default-patterns # to see the built-in patterns
    ./target/debug/hustlog -i /var/log/system.log -p SYSLOGLINE -s "+timestamp:ts:%b %e %H:%M:%S" -s +message -m

Using SQL:

    /target/debug/hustlog  -i /var/log/system.log -p SYSLOGLINE \
        -s "+timestamp:ts:%b %e %H:%M:%S" -s +message -m \
        -q 'select * from SYSLOGLINE where message="ASL Sender Statistics" limit 3 offset 1;'
    2022-04-27 00:25:39 +02:00,ASL Sender Statistics
    2022-04-27 00:42:19 +02:00,ASL Sender Statistics
    2022-04-27 00:57:19 +02:00,ASL Sender Statistics

DATE function can be used to specify time instants

    ./target/debug/hustlog  -i /var/log/system.log -p SYSLOGLINE \
    -s "+timestamp:ts:%b %e %H:%M:%S" -s +message -m \
    -q 'select * from SYSLOGLINE where message="ASL Sender Statistics" \
            and timestamp > DATE("%b %e %H:%M:%S", "Apr 27 12:00:00") \
            limit 3 offset 1;'
