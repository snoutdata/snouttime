# The fuzzing environment (docs/snouttime/PLAN.md 3.2, R5): the dev image plus a pinned
# NIGHTLY toolchain and cargo-fuzz, because libFuzzer's sanitizer flags are nightly-only.
# The nightly is used for `cargo fuzz` and nothing else; the extension is built and tested
# with the pinned stable toolchain (D1). Decided 2026-09-23.
ARG DEV_IMAGE=snouttime-dev
FROM ${DEV_IMAGE}
ARG NIGHTLY=nightly-2026-09-15
ARG CARGO_FUZZ_VERSION=0.13.2
RUN rustup toolchain install "${NIGHTLY}" --profile minimal \
	&& cargo +"${NIGHTLY}" install --locked cargo-fuzz --version "${CARGO_FUZZ_VERSION}"
ENV SNOUTTIME_NIGHTLY=${NIGHTLY}
RUN mkdir -p /cache/fuzz-target
