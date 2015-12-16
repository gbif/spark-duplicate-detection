logLevel := Level.Warn

resolvers += Resolver.url(
  "bintray-sbt-plugin-releases",
  url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
  Resolver.ivyStylePatterns)

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.1.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
