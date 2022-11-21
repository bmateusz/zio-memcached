# ZIO Memcached

| Project Stage | CI | Release | Snapshot | Discord |
| --- | --- | --- | --- | --- |
| [![Project stage][Stage]][Stage-Page] | ![CI][Badge-CI] | [![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] | [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots] | [![Discord][Badge-Discord]][Link-Discord] |

ZIO Memcached is a type-safe, performant, ZIO native Memcached client.

> The client is still a work-in-progress. Watch this space!

[ZIO Redis](https://github.com/zio/zio-redis/) by Dejan Mijic inspired this project. Instead of starting from scratch, I forked ZIO Redis and transformed it into a Memcached client.

# Documentation

Learn more on the [ZIO Memcached Microsite](https://zio.github.io/zio-memcached/)!

# Getting started

See the [example](example/src/main) project, which is a fully featured client using [ZIO HTTP](https://github.com/zio/zio-http).

To run the tests, the example, or the benchmarks, you have to run two Memcached instances with the default port (11211) and 11212.

You can run Memcached using Docker, starting the two instances with `docker-compose up -d`. When finished, you can stop them with `docker-compose down -v`.

If you prefer running Memcached natively, you can also download it using your package manager (e.g., `brew install memcached`), and start two instances with `memcached` and `memcached -p 11212`. No additional configuration is needed. If you want to debug, you can increase the verbosity with the `-vv` parameter.

# Contributing

[Documentation for contributors](https://zio.github.io/zio-memcached/docs/about/about_contributing)

## Code of Conduct

See the [Code of Conduct](https://zio.github.io/zio-memcached/docs/about/about_coc)

## Support

Come chat with us on [![Badge-Discord]][Link-Discord].

# License

[License](LICENSE)

[Badge-CI]: https://github.com/zio/zio-memcached/workflows/CI/badge.svg
[Badge-Discord]: https://img.shields.io/discord/629491597070827530?logo=discord
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-memcached_2.12.svg
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/oss.sonatype.org/dev.zio/zio-memcached_2.12.svg
[Link-Discord]: https://discord.gg/2ccFBr4
[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-memcached_2.12/
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-memcached_2.12/
[Stage]: https://img.shields.io/badge/Project%20Stage-Experimental-yellow.svg
[Stage-Page]: https://github.com/zio/zio/wiki/Project-Stages
