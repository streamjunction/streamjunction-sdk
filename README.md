# Using Streamjunction SDK

Streamjunction SDK was designed to be used with the Storm applications deployed
with the Storm-as-a-Service platform [Stream Junction](https://www.streamjunction.com).

To use it you need to include three pieces into your `pom.xml`.

First, register the repositories:

    <repositories>
      ...
      <repository>
        <id>release.streamjunction.com</id>
        <url>s3://repo.maven.streamjunction/release</url>
      </repository>
      <repository>
        <id>snapshot.streamjunction.com</id>
        <url>s3://repo.maven.streamjunction/snapshot</url>
      </repository>
    </repositories>
    
Then, use the following wagon extension to access the S3
resources:

    <build>    
      <extensions>  
        <extension>  
          <groupId>org.springframework.build.aws</groupId>  
          <artifactId>org.springframework.build.aws.maven</artifactId>  
          <version>3.0.0.RELEASE</version>  
        </extension>
      </extensions>
      ...
    </build>

And finally, add the dependency:

    <dependencies>
      <dependency>
        <groupId>com.streamjunction.sdk</groupId>
        <artifactId>sdk</artifactId>
        <version>1.0-SNAPSHOT</version>
      </dependency>
      ..
    </dependencies>
