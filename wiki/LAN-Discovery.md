# LAN Discovery (Bonjour/mDNS)

`web.bonjour` is opt-in, zero-config LAN discovery: the daemon advertises
its [web control API](HTTP-API) as a `_cronstable._tcp` service over
mDNS/Bonjour, so a companion app (or a service browser) on the same network
finds it without a typed URL.

## What is advertised

The advert carries:

- the **instance name**: the node's hostname by default, or the map form's
  `name:` override (dots are replaced with hyphens; the label is truncated
  to 63 characters);
- the **port**: the actually bound TCP port of the web listener, so it is
  correct even for an ephemeral `listen: http://127.0.0.1:0`;
- TXT records `v` (the daemon version) and `scheme` (`http`, or `https`
  when any listen address uses it).

The advert carries no secrets: name, port, scheme, and version only. A
client that discovers the daemon still needs a bearer token to read
anything (see [Authentication](HTTP-API#authentication)).

## Configuration

Bonjour requires the `discovery` extra:

```shell
pip install "cronstable[discovery]"
```

(The extra is python-zeroconf. The release binaries bundle it best-effort
per architecture.)

The boolean form advertises under the hostname:

```yaml
web:
  listen:
    - http://0.0.0.0:8080
  bonjour: true
```

The map form overrides the instance name (and `enabled: false` turns the
advert off while keeping the block):

```yaml
web:
  listen:
    - http://0.0.0.0:8080
  bonjour:
    enabled: true
    name: cron-prod-1
```

## Validation

Both checks fail at parse time (`ConfigError`), so `--validate-config`
catches them:

- `web.bonjour` enabled without python-zeroconf installed: install the
  `discovery` extra or disable `web.bonjour`.
- `web.bonjour` enabled when every `web.listen` entry is a unix socket:
  the advert needs a TCP listener to point at.

## Runtime behavior

The advert follows the web app's lifecycle: it is registered while (and
only while) a TCP listener is actually bound, is updated on a config reload
when anything it carries changed, and is withdrawn on shutdown. A runtime
mDNS failure (a registration error, no non-loopback address to advertise)
is logged and the advert is skipped until the next config apply; discovery
is a convenience and never takes down the scheduler.

## Browsing

macOS:

```shell
dns-sd -B _cronstable._tcp
```

Linux (avahi):

```shell
avahi-browse -r _cronstable._tcp
```

## Related pages

- [HTTP Control API](HTTP-API): the interface the advert points at
- [Push Notifications](Push-Notifications): pairing the companion app the advert helps find
- [Web Dashboard](Web-Dashboard)
- [Configuration Reference](Configuration-Reference)
