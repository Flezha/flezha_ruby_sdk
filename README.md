# flezha

The Flezha Ruby SDK, a poll host over `flz_core`, the runner kernel the Rust,
Java, Node, Python, PHP and Elixir SDKs also drive. The kernel owns transport,
ordering, per-cell exclusivity, durability, retries, timers, credit, routing
and steps runs. This gem brings the authoring surface, the MessagePack codec
and a thread host that releases the GVL around every blocking core call.

**Support tier:**
[tier 2](https://github.com/flezha/flezha/blob/main/docs/public/publishing/maturity.md#sdk-ruby),
passes the full conformance run in CI exactly as tier 1 does, with every capability
present, and is permanently supported; it is not yet run by the maintainer in a live
deployment, so a defect specific to it is likely to be found by its users first.

[`examples/echo`](examples/echo/README.md) is the smallest complete runner.

## Install

```bash
gem install flezha
```

or `gem "flezha", "0.1.0"` in a `Gemfile`. The gem is not on RubyGems yet. Until
it is, build and install it from a checkout as
[`examples/echo`](examples/echo/README.md#building-from-a-checkout) shows.

## Supported Ruby versions

Ruby **3.3 or newer**. The startup order calls `Process.warmup`, which is 3.3 and
later, and Ruby 3.2 is past end of life. YJIT is enabled where the running Ruby
has it, and the startup line reports it either way.

## The native library

A release publishes a precompiled gem for `x86_64-linux`, `aarch64-linux`,
`x86_64-darwin`, `arm64-darwin` and `x64-mingw-ucrt`, which carries the native
library, so installing it needs no Rust toolchain. On any other platform the
source gem builds the library at install, which needs a stable Rust toolchain
and git: it fetches the core at the tag `ext/flz_core_ffi/Cargo.toml` names.

## Local mode, testing and the relay API

- **Local mode**: `Flezha::LocalRelay.start` runs a relay's declared graph in
  your own process, with no controller. See
  [Local mode](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode).
- **Testing**: `Flezha::Testing::Harness`, after `require "flezha/testing"`,
  drives one tasklet and checks its replay. See
  [Check a tasklet on its own](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own).
- **The relay API**: a relay's entry channels, answering a caller, `call(...)`
  to another relay, and `runner.send_to` and `runner.call` from application
  code. See
  [Sending and calling](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/sending_and_calling.md).

Full documentation:
[the Ruby SDK guide](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/ruby_sdk.md).

Licensed under Apache-2.0, [`LICENSE`](LICENSE).
