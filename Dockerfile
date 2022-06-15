####################################################################################################
## Builder
####################################################################################################
FROM almalinux:8 AS builder

RUN update-ca-trust

RUN dnf -y groupinstall "Development Tools"
RUN dnf install -y wget

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.61.0

RUN set -eux; \
    myArch="$(arch)"; \
    case "${myArch##*-}" in \
        amd64|x86_64) rustArch='x86_64-unknown-linux-gnu'; rustupSha256='3dc5ef50861ee18657f9db2eeb7392f9c2a6c95c90ab41e45ab4ca71476b4338' ;; \
        armhf) rustArch='armv7-unknown-linux-gnueabihf'; rustupSha256='67777ac3bc17277102f2ed73fd5f14c51f4ca5963adadf7f174adf4ebc38747b' ;; \
        arm64) rustArch='aarch64-unknown-linux-gnu'; rustupSha256='32a1532f7cef072a667bac53f1a5542c99666c4071af0c9549795bbdb2069ec1' ;; \
        i386) rustArch='i686-unknown-linux-gnu'; rustupSha256='e50d1deb99048bc5782a0200aa33e4eea70747d49dffdc9d06812fd22a372515' ;; \
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    url="https://static.rust-lang.org/rustup/archive/1.24.3/${rustArch}/rustup-init"; \
    wget "$url"; \
    echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $RUST_VERSION --default-host ${rustArch}; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    rustup --version; \
    cargo --version; \
    rustc --version;


RUN dnf install -y epel-release

RUN dnf install -y clang unixODBC-devel

# Create appuser
ENV USER=hustlog
ENV UID=10001

#    --disabled-password \
#    --gecos "" \
RUN adduser \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"


WORKDIR /hustlog

# (Hopefully) cache deps across rebuilds
RUN mkdir src && touch src/main.rs
COPY ./Cargo.toml ./Cargo.lock ./
RUN cargo fetch

COPY ./src/ ./src/

RUN cargo build --release

####################################################################################################
## Final image
####################################################################################################
FROM almalinux:8

RUN dnf install -y llvm-libs unixODBC mysql-devel
RUN dnf install -y wget

RUN mkdir /tmp/odbc_package && \
    cd /tmp/odbc_package && \
      wget https://downloads.mariadb.com/Connectors/odbc/connector-odbc-3.1.7/mariadb-connector-odbc-3.1.7-ga-rhel8-x86_64.tar.gz && \
      tar -xvzf mariadb-connector-odbc-3.1.7-ga-rhel8-x86_64.tar.gz && \
      install lib64/libmaodbc.so /usr/lib64/ && \
      install -d /usr/lib64/mariadb/ && \
      install -d /usr/lib64/mariadb/plugin/ && \
      install lib64/mariadb/plugin/auth_gssapi_client.so /usr/lib64/mariadb/plugin/ && \
      install lib64/mariadb/plugin/caching_sha2_password.so /usr/lib64/mariadb/plugin/ && \
      install lib64/mariadb/plugin/client_ed25519.so /usr/lib64/mariadb/plugin/ && \
      install lib64/mariadb/plugin/dialog.so /usr/lib64/mariadb/plugin/ && \
      install lib64/mariadb/plugin/mysql_clear_password.so /usr/lib64/mariadb/plugin/ && \
      install lib64/mariadb/plugin/sha256_password.so /usr/lib64/mariadb/plugin/

# Import from builder.
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

WORKDIR /hustlog

# Copy our build
COPY --from=builder /hustlog/target/release/hustlog ./

RUN ldd /hustlog/hustlog

COPY ./config_examples/ /etc/hustlog/

# Use an unprivileged user.
USER hustlog:hustlog

ENTRYPOINT ["/hustlog/hustlog"]
CMD /hustlog/hustlog

