# Changelog

## [0.4.1](https://github.com/joshrotenberg/redis_server_wrapper/compare/v0.4.0...v0.4.1) (2026-04-06)


### Bug Fixes

* add stale process cleanup to managed mode and guard Port.close ([#14](https://github.com/joshrotenberg/redis_server_wrapper/issues/14)) ([5cf69ac](https://github.com/joshrotenberg/redis_server_wrapper/commit/5cf69ac6f77fcb1b4ad5b6cd580032a7b398e98e))

## [0.4.0](https://github.com/joshrotenberg/redis_server_wrapper/compare/v0.3.0...v0.4.0) (2026-04-06)


### Features

* add managed option for Port-based process lifecycle ([#12](https://github.com/joshrotenberg/redis_server_wrapper/issues/12)) ([6be3284](https://github.com/joshrotenberg/redis_server_wrapper/commit/6be32846bd434c65b92dcd20049f93402ec9264e))

## [0.3.0](https://github.com/joshrotenberg/redis_server_wrapper/compare/v0.2.0...v0.3.0) (2026-04-02)


### Features

* add Chaos module for fault injection ([#5](https://github.com/joshrotenberg/redis_server_wrapper/issues/5)) ([bd7bd78](https://github.com/joshrotenberg/redis_server_wrapper/commit/bd7bd78c4661c063efbb9d722f0565f9f8cc20ae))


### Bug Fixes

* match on {:ok, "ERR..."} since redis-cli exits 0 for Redis errors ([#10](https://github.com/joshrotenberg/redis_server_wrapper/issues/10)) ([d07a699](https://github.com/joshrotenberg/redis_server_wrapper/commit/d07a69901627d51a186469570c761e0d34004f0e)), closes [#9](https://github.com/joshrotenberg/redis_server_wrapper/issues/9)
* trigger_failover auto-falls back to FORCE when master is down ([#8](https://github.com/joshrotenberg/redis_server_wrapper/issues/8)) ([c36ca3c](https://github.com/joshrotenberg/redis_server_wrapper/commit/c36ca3c76b06dc74e4647d117170a6f81f885b08)), closes [#7](https://github.com/joshrotenberg/redis_server_wrapper/issues/7)

## [0.2.0](https://github.com/joshrotenberg/redis_server_wrapper/compare/v0.1.0...v0.2.0) (2026-04-02)


### Features

* prepare for hex.pm publication ([#1](https://github.com/joshrotenberg/redis_server_wrapper/issues/1)) ([0dbc557](https://github.com/joshrotenberg/redis_server_wrapper/commit/0dbc557bb3a212ac9e81872814511ab9577430ee))
