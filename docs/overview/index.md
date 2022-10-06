---
id: overview_index
title: "Summary"
---

ZIO Memcached is a type-safe, performant, ZIO native Memcached client.

## Installation

Include ZIO Memcached in your project by adding the following to your build.sbt file:

```scala mdoc:passthrough
println(s"""```""")
if (zio.memcached.BuildInfo.isSnapshot)
  println(s"""resolvers += Resolver.sonatypeRepo("snapshots")""")
println(s"""libraryDependencies += "dev.zio" %% "zio-memcached" % "${zio.memcached.BuildInfo.version}"""")
println(s"""```""")
```
