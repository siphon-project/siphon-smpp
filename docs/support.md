# Commercial support

siphon-smpp is MIT-licensed and free to run in production — no open-core
holdbacks, no paid tier of the SMPP engine. If you'd rather not carry the
integration and operations alone, commercial support is available from
**[Real Time Telecom B.V.](https://realtime-telecom.nl)**, run by the addon's
maintainer.

## What RTT can help with

- **SMSC design & deployment** — sizing and topology for a store-and-forward
  SMSC, from a single node to an HA pair / N nodes. See
  [Deployment](deployment.md) and [Kubernetes & scaling](kubernetes.md) for the
  shape this builds on.
- **Upstream / aggregator integration** — getting outbound binds, throttling,
  DLR correlation and routing working reliably against real aggregators and
  their quirks.
- **Custom scripting & feature development** — Python handlers built to your call
  flow (routing tables, LCR, store-and-forward queueing, persistence),
  upstreamed into the project where it makes sense.
- **Bespoke SMS platform builds** — higher-level SMS services on top of the
  commodity SMPP core, delivered as a services engagement.
- **Performance tuning** — profiling and capacity planning against your real
  traffic mix, including free-threaded CPython builds. See
  [Performance](performance.md).
- **SLA-backed support** — production response commitments.

[Get in touch via realtime-telecom.nl →](https://realtime-telecom.nl)

## Sponsor the project

Want a particular feature built or fast-tracked? Feature sponsorship funds work
that lands in the open-source project — your use case ships sooner, and everyone
downstream benefits. Use the **Sponsor** button on the
[GitHub repository](https://github.com/siphon-project/siphon-smpp), or reach out
through RTT to scope it.

Ongoing development is backed by
**[Real Time Telecom B.V.](https://realtime-telecom.nl)**, which also provides the
[`smpp34`](https://github.com/Real-Time-Telecom-B-V/smpp34) SMPP codec siphon-smpp
is built on.
