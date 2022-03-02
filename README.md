ball-wagon-providers
====================


Description
-----------

[Apache Maven] [Wagon][Apache Maven Wagon] providers for [AWS S3] and
[GCP Cloud Storage].

Javadoc is published at
<https://allen-ball.github.io/ball-wagon-providers/>.

To load, in `${user.home}/.m2/settings.xml`:

```xml
      <pluginRepositories>
        ...
        <pluginRepository>
          <id>repo.hcf.dev-RELEASE</id>
          <url>https://repo.hcf.dev/maven/release/</url>
        </pluginRepository>
        ...
      </pluginRepositories>
```

And in `pom.xml`:

```xml
  <build>
    ...
    <extensions>
      <extension>
        <groupId>ball</groupId>
        <artifactId>ball-wagon-providers</artifactId>
        <version>3.2.2.20220227</version>
      </extension>
    </extensions>
    ...
  </build>
```


License
-------

This code is under the [Apache License, Version 2.0, January 2004].


[Apache Maven]: https://maven.apache.org/
[Apache Maven Wagon]: https://maven.apache.org/wagon/

[AWS S3]: https://aws.amazon.com/pm/serv-s3/

[GCP Cloud Storage]: https://cloud.google.com/storage/

[Apache License, Version 2.0, January 2004]: https://www.apache.org/licenses/LICENSE-2.0
