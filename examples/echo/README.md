# Echo: the Ruby hello world

The Ruby hello world, as a standalone project. Nothing here depends on the
Flezha source tree: the `Gemfile` names one released version of `flezha`, the
way your own project would. It needs Ruby 3.3 or later.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.rb` | the tasklet | the processing stage: prefixes each message |
| `echo_operator.rb` | the operator | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.rb` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
bundle install
bundle exec ruby runner.rb
```

Then, from another terminal:

```sh
flz operator start echo --id my_echo
flz operator send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It keeps running until a SIGTERM, Ctrl-C, or its standard input closing, so do
not start it with standard input closed. The namespace is the first argument
and defaults to `default`: `bundle exec ruby runner.rb my_app`.

## Building from a checkout

The gem is not on RubyGems yet. From the SDK's repository root, with the Ruby
you will run the runner with, build a gem for this platform and install it once:

```sh
rake native gem
gem install pkg/flezha-0.1.0-*.gem
```

`rake native gem` compiles the SDK's native library from the core's git tag, so
it needs cargo and git. Once the gem is installed, `bundle install` in this
folder finds it, and once it is published the step goes away.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. [`docs/sdk/ruby_sdk.md`](../../../../docs/sdk/ruby_sdk.md) covers the rest of
the SDK.
