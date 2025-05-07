## [1.0.6](https://github.com/iggy-rs/iggy-node-client/compare/v1.0.5...v1.0.6) (2025-01-14)


### Bug Fixes

* add commitlint as husky pre-commit hook and ci check ([#25](https://github.com/iggy-rs/iggy-node-client/issues/25)) ([5dbac10](https://github.com/iggy-rs/iggy-node-client/commit/5dbac1071bea5c26f852a60361ac51217df09d25))

## [1.0.5](https://github.com/iggy-rs/iggy-node-client/compare/v1.0.4...v1.0.5) (2025-01-14)


### Bug Fixes

* set package.json license to Apache-2.0 as well ([#24](https://github.com/iggy-rs/iggy-node-client/issues/24)) ([2523c0f](https://github.com/iggy-rs/iggy-node-client/commit/2523c0fde82958e8a13bb95b2c2d0babe0d1d290))

## [1.0.4](https://github.com/iggy-rs/iggy-node-client/compare/v1.0.3...v1.0.4) (2024-12-16)


### Bug Fixes

* add tests, fix headers.double type, ehance typing ([431063f](https://github.com/iggy-rs/iggy-node-client/commit/431063f253e0fb1739188bbd6614cfc06ccb3fd4))

## [1.0.3](https://github.com/iggy-rs/iggy-node-client/compare/v1.0.2...v1.0.3) (2024-11-24)


### Bug Fixes

* upgrade dependencies ([#19](https://github.com/iggy-rs/iggy-node-client/issues/19)) ([a05d5c2](https://github.com/iggy-rs/iggy-node-client/commit/a05d5c2484f8711f72a23c4ee1222d29e323a5ba))

## [1.0.2](https://github.com/iggy-rs/iggy-node-client/compare/v1.0.1...v1.0.2) (2024-11-24)


### Bug Fixes

* **npm:** configure package as public ([#18](https://github.com/iggy-rs/iggy-node-client/issues/18)) ([7a56455](https://github.com/iggy-rs/iggy-node-client/commit/7a5645512a564322af343bc7ba748c8c58dfab1e))

## [1.0.1](https://github.com/iggy-rs/iggy-node-client/compare/v1.0.0...v1.0.1) (2024-11-24)


### Bug Fixes

* **npm:** publish package ([#17](https://github.com/iggy-rs/iggy-node-client/issues/17)) ([6e2f60e](https://github.com/iggy-rs/iggy-node-client/commit/6e2f60e2f4484b57596285ff79d76ea647962595))

# 1.0.0 (2024-11-24)


### Bug Fixes

* add e2e tests, fix create-group return ([ad52b43](https://github.com/iggy-rs/iggy-node-client/commit/ad52b43a6ee5e8f868eb1178a9b9f02f11cb1204))
* add package's keywords, cleans up logs ([5287b74](https://github.com/iggy-rs/iggy-node-client/commit/5287b74006733aef4429d889c0cc38a80f375dc2))
* correctly destroy socket when destroy function is called on TCPClient ([76f7d07](https://github.com/iggy-rs/iggy-node-client/commit/76f7d07a96e44f6701998bb9fb2dab8fa4d75489))
* enforce check on message id to be either uuid string or 0 ([d05b898](https://github.com/iggy-rs/iggy-node-client/commit/d05b898d62b097ca9770cdab289707546c2fe6aa))
* fix client auth quirks, add handleResponse & deserializePollMessage as transform stream ([0458d57](https://github.com/iggy-rs/iggy-node-client/commit/0458d579de45605588fff7b7d01119c9625364da))
* fix consumer group commands ([a33094f](https://github.com/iggy-rs/iggy-node-client/commit/a33094f66eb2e433b2fc8cc6adf7f6a122b0f365))
* fix createtopic, add new compressionAlgorithm param ([f93a444](https://github.com/iggy-rs/iggy-node-client/commit/f93a4441f4bcd790763d982ead981c4ac86b33c5))
* fix getStats command (add new totalCpuUsage field) ([#3](https://github.com/iggy-rs/iggy-node-client/issues/3)) ([de4cfda](https://github.com/iggy-rs/iggy-node-client/commit/de4cfdad4046f556a51878fbadb8dde0df9302c5))
* fix github ci ([78870e3](https://github.com/iggy-rs/iggy-node-client/commit/78870e389333c8b7ca2425748c8806fa5e36a7ae))
* fix message header typing ([1276374](https://github.com/iggy-rs/iggy-node-client/commit/12763749f95d29a78028f95b5bc33281a62246c9))
* fix message headers serialization bug ([22ffe16](https://github.com/iggy-rs/iggy-node-client/commit/22ffe1603db9b7a94deadb1fcf6a25f81cfea868))
* fix module type export ([b0bcdc7](https://github.com/iggy-rs/iggy-node-client/commit/b0bcdc7945ba68d519a129ef33b4353538e9d64e))
* fix npm test command for ci ([842fe69](https://github.com/iggy-rs/iggy-node-client/commit/842fe697548224a08012574d2e44f14100105b63))
* fix Partitioning.MessageKey type, fix indent ([05d05b6](https://github.com/iggy-rs/iggy-node-client/commit/05d05b6db68b33bc9045fee16c9fca8c8eb0ae6d))
* fix tcp client options ([f9bd442](https://github.com/iggy-rs/iggy-node-client/commit/f9bd44204f86f2becfe674c9b38f09657c00ac6c))
* fix topic deserialisation bug ([3e787be](https://github.com/iggy-rs/iggy-node-client/commit/3e787be2546f29f1f8d31e9e1388e19a62729e50))
* fix updateUser and changePassword command ([d01e086](https://github.com/iggy-rs/iggy-node-client/commit/d01e08621bbf976c7dc5578273a253a0dcc43e72))
* fix var naming, add some test ([40d91dd](https://github.com/iggy-rs/iggy-node-client/commit/40d91ddfe41f6114ca3c7da2a30225e81e3226bc))
* get rid of enums, add type helpers ([6e2613b](https://github.com/iggy-rs/iggy-node-client/commit/6e2613b2f1ab0112d401f87a2f0cfb6e77b8d99d))
* more e2e tests ([d537aa7](https://github.com/iggy-rs/iggy-node-client/commit/d537aa7454b983edb471fa354e55b5e814fb1524))
* no ssh pull ([#12](https://github.com/iggy-rs/iggy-node-client/issues/12)) ([f13f0ed](https://github.com/iggy-rs/iggy-node-client/commit/f13f0edd5adf4584ab7f02620a159317654251e5))
* remove bad symbol in the CI definition ([1878a0c](https://github.com/iggy-rs/iggy-node-client/commit/1878a0c501224d17eefc986ccd5d89e481cc15b8))
* remove console.error on normal close signal ([0f8c993](https://github.com/iggy-rs/iggy-node-client/commit/0f8c99330b2352220ee23df0b7923185d710cd89))
* remove initial .gitignore and README.md to avoid rebase conflict ([931b9b8](https://github.com/iggy-rs/iggy-node-client/commit/931b9b8f5d0b8a249de8a30c3be9435c93a8461f))
* update createUser, createStream & createTopic command return value ([8712b0f](https://github.com/iggy-rs/iggy-node-client/commit/8712b0f5021b7852361117f6fc764eac3851beb7))
* update readme ([263f271](https://github.com/iggy-rs/iggy-node-client/commit/263f271fed2c529b89329e88d83fc4100b04c639))
* use debug lib, make poolsize configurable ([2f66e89](https://github.com/iggy-rs/iggy-node-client/commit/2f66e89220b30b3e33aa773b6471b1669658f37f))
* use pat as token for semantic release ([#10](https://github.com/iggy-rs/iggy-node-client/issues/10)) ([5eccfd2](https://github.com/iggy-rs/iggy-node-client/commit/5eccfd281763773140ce9be39a2c022b2de803b6))


### Features

* add base ci workflow ([#2](https://github.com/iggy-rs/iggy-node-client/issues/2)) ([4cbde14](https://github.com/iggy-rs/iggy-node-client/commit/4cbde140409841bf3d440f01ccaca1a855f37b13))
* add command client with socket pool management ([e747b29](https://github.com/iggy-rs/iggy-node-client/commit/e747b292374a7a8e1ee8f27d69a9203db3a6b09b))
* add CommandResponseStream to wrap tcp socket, add parallel call safetiness ([0e6f38f](https://github.com/iggy-rs/iggy-node-client/commit/0e6f38fde621dc1cfe3f12376723f1b43ffee9bf))
* add consumer stream facility ([8c19d3f](https://github.com/iggy-rs/iggy-node-client/commit/8c19d3fae30a5ab47d0f10b4747158e604aea37f))
* add create, delete, join & leave consumer-group command ([e7ca376](https://github.com/iggy-rs/iggy-node-client/commit/e7ca3762a9fcb92a3bb5018cb66570079bec60f1))
* add createPartition & deletePartition command ([2286c10](https://github.com/iggy-rs/iggy-node-client/commit/2286c106534704b02544613944e74f2549ca1a67))
* add createUser and deleteUser command ([d25d837](https://github.com/iggy-rs/iggy-node-client/commit/d25d837d38abe89a038880651c19bd8925f1affe))
* add getGroup and getGroups command ([cdfa766](https://github.com/iggy-rs/iggy-node-client/commit/cdfa766c9b800e59c2858599ccf87afa44c142dc))
* add getOffset and storeOffset command, fix typos ([6f9a425](https://github.com/iggy-rs/iggy-node-client/commit/6f9a4256b713abcfdd6dc146b78268169c9bb198))
* add pollMessage command ([fe59f82](https://github.com/iggy-rs/iggy-node-client/commit/fe59f825ebb0ba3a4c4c53e90cd8a28f9b21cca2))
* add purgeTopic & purgeStream command ([86482d1](https://github.com/iggy-rs/iggy-node-client/commit/86482d12bfdf48b23973374e09cb9cc5e852ef18))
* add SendMessages command ([e1d39d8](https://github.com/iggy-rs/iggy-node-client/commit/e1d39d887e36783e2ca23cd560060df3fc62099a))
* add updateStream command ([89970c0](https://github.com/iggy-rs/iggy-node-client/commit/89970c0574f0bcade634b338522bcbe6990b4b43))
* add updateTopic command ([f1e278e](https://github.com/iggy-rs/iggy-node-client/commit/f1e278e993a05c10f10a24761bd2e63f95d9891b))
* add updateUser and changePassword command, fix permissions deserialization bug ([f3bcda3](https://github.com/iggy-rs/iggy-node-client/commit/f3bcda30b71f5d70145257534848ee4053e714f7))
* better error, add some test ([f386854](https://github.com/iggy-rs/iggy-node-client/commit/f386854bd388e95a11251803dcb81292b33d7981))
* publish to npm ([82268f5](https://github.com/iggy-rs/iggy-node-client/commit/82268f5963e72865355e2cfe919307d5bd1500ab))
* reorganize client declaration ([7cb1bd8](https://github.com/iggy-rs/iggy-node-client/commit/7cb1bd857c5d1050ab6b8d2b0e6b70908b3bc105))
* start low level command api and base tcp client ([7bb8b32](https://github.com/iggy-rs/iggy-node-client/commit/7bb8b32ec809e53d665295f9e0c069d535b88cae))
* start unit test on serialization ([2082b71](https://github.com/iggy-rs/iggy-node-client/commit/2082b713a37d59a3d0482669a3a544a6a922ae41))
* update modified commands for v0.3.0 server release (createTopic, updateTopic, login, createToken) ([cb4f0d1](https://github.com/iggy-rs/iggy-node-client/commit/cb4f0d17c61967e95a6b5502c70d6e8b8569e9c7))
* wraps command to higher level api, starts client ([60f9466](https://github.com/iggy-rs/iggy-node-client/commit/60f9466cf92965f3bf4ff5918e50876e5bb9aa84))
