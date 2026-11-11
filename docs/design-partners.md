# Design partners

ANKAA is young, and it gets better fastest on real data. A design partner is a team that runs it on
one of its own feeds for a few weeks and tells us what breaks, what is missing and what matters.

## Who it is for

Teams that store and query time series that arrive continuously, for example:

- **Plants and industrial IoT:** machines and lines publishing over MQTT, OPC UA gateways, a Unified
  Namespace, a historian that is expensive or hard to query.
- **Energy and metering:** meters, inverters, batteries, grid assets; quarter-hour aggregates and
  long retention.
- **Fleets and sensors:** many devices, each with its own clock, connectivity gaps and late data.
- **Market data:** trades, quotes and order books, ASOF joins and bars, replay of captures.
- **Platforms:** a busy Kafka or NATS topic that should be queryable as a table.

## What you get

- Hands-on help to run ANKAA on a copy of one feed: configuration, mapping your messages, sizing.
- Your missing features first in the queue, built in the open.
- A direct line to the maintainer, answers within a working day.
- All of it at no cost. ANKAA is Apache-2.0; nothing here changes that.

## What we ask

- A 20-minute call to start, then a short call every two weeks while you evaluate.
- Honest feedback, including "this is not for us, because…".
- Permission to describe your use case, anonymously unless you say otherwise.
- When it works for you, a short written note saying so. That is the most valuable thing a young
  project can receive.

No data leaves your systems: ANKAA runs on your machines, and we only see what you choose to share.

## How to start

1. Try it: the [factory demo](../examples/factory/) runs with one command, or follow the
   [Unified Namespace tutorial](tutorials/uns-historian.md).
2. Open a [GitHub Discussion](https://github.com/ualsweb/ankaa/discussions) with the *Design partner*
   label and a few lines about your data (what, how much, from where), or write to
   `hola@urtzialfaro.dev` if you prefer not to post in public.
3. We set up the first call.

What ANKAA is today, including what it does not do yet, is in [STATUS](STATUS.md). Please read it
first: we would rather you find a gap there than in production.
