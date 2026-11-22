# Management UI and API

`GET /` serves a one-page UI that refreshes every two seconds, with a purge button per queue. The JSON API is read-only except for purge, and has no authentication.

```text
GET    /api/{overview,queues,exchanges,bindings,connections}
DELETE /api/queues/%2F/{name}/contents        # purge ready messages
```

The fault-injection endpoints live under `/chaos` on the same port. See [fault injection](fault-injection.md).

This is not a drop-in RabbitMQ management API.
