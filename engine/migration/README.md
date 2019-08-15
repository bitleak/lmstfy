## Migration Engine
This engine is a proxy-like engine for the `redis` engine.

It's main purpose is to migrate a redis engine to a larger new redis engine without
breaking existing clients. The migration engine will send all PUBLISH operation to
the new redis engine, and CONSUME from both engines.

The authorization token used for the migration is the same as the old pool. After
migration finished, we can update the config, remove the old pool and rename the new
pool with the old name. The user does NOT needed to do anything.
