# Hustlog

Disclaimer: This is my first non-trivial Rust project and I am using it to learn the language.

The goal of the project is to become a "swiss army knife" for logs, maybe even "ffmpeg for logs"

What it has for now: 

- log parser based on regular expressions and the [grok](https://crates.io/crates/grok) crate
- (partially supported/WIP) SQL interface to access the parsed data
(query parsing is using the [sqlparser](https://crates.io/crates/sqlparser) crate)


## Use cases

* query a log file using sql interface
  * example:
       TODO
  * limitations:
    * ...

* parse a log file and output (batched) SQL insert statements


### Examples

Example usage:

    cargo build
    ./target/debug/hustlog --help # not everything is implemented
    ./target/debug/hustlog --grok-list-default-patterns # to see the built-in patterns
    ./target/debug/hustlog -i /var/log/system.log -g SYSLOGLINE -s "+timestamp:ts:%b %e %H:%M:%S" -s +message -m

Using SQL:

    /target/debug/hustlog  -i /var/log/system.log -g SYSLOGLINE \
        -s "+timestamp:ts:%b %e %H:%M:%S" -s +message -m \
        -q 'select * from SYSLOGLINE where message="ASL Sender Statistics" limit 3 offset 1;'
    2022-04-27 00:25:39 +02:00,ASL Sender Statistics
    2022-04-27 00:42:19 +02:00,ASL Sender Statistics
    2022-04-27 00:57:19 +02:00,ASL Sender Statistics

DATE function can be used to specify time instants

    ./target/debug/hustlog  -i /var/log/system.log -g SYSLOGLINE \
    -s "+timestamp:ts:%b %e %H:%M:%S" -s +message -m \
    -q 'select * from SYSLOGLINE where message="ASL Sender Statistics" \
            and timestamp > DATE("%b %e %H:%M:%S", "Apr 27 12:00:00") \
            limit 3 offset 1;'


Output SQL Insert batches (e.g. to be piped to mysql client):

    ./target/debug/hustlog  -i /var/log/system.log -g SYSLOGLINE \
    -s "+timestamp:ts:%b %e %H:%M:%S" \
    -s logsource \
    -s program \
    -s pid:int \
    -s +message \
    -m \
    --output-format sql \
    --output-add-ddl
    
        CREATE TABLE SYSLOGLINE (
        timestamp TIMESTAMP ,
        logsource VARCHAR ,
        program VARCHAR ,
        pid BIGINT ,
        message VARCHAR ) ;
        
        INSERT INTO SYSLOGLINE (timestamp,logsource,program,pid,message)
        VALUES
        ('2022-05-10 00:11:59 +02:00','actek-mac','syslogd',106,'ASL Sender Statistics'),
        ('2022-05-10 00:26:07 +02:00','actek-mac','syslogd',106,'ASL Sender Statistics'),
        ('2022-05-10 00:30:05 +02:00','actek-mac','syslogd',106,'Configuration Notice:'),
        ('2022-05-10 00:30:05 +02:00','actek-mac','syslogd',106,'Configuration Notice:'),
        ('2022-05-10 00:30:05 +02:00','actek-mac','syslogd',106,'Configuration Notice:'),


Also supports SQL filter/transformation using -q, e.g.

    ./target/debug/hustlog  -i /var/log/system.log -g SYSLOGLINE \
    -s "+timestamp:ts:%b %e %H:%M:%S" \
    -s logsource \
    -s program \
    -s pid:int \
    -s +message \
    -m \
    -q 'select timestamp,logsource,pid,message from SYSLOGLINE where program="syslogd"' \
    --output-format sql \
    --output-batch-size 2000 \
    --output-add-ddl

