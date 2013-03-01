# Scaliak

Scaliak is a scala-ified version of the High-Level Riak Java Client w/ a Functional Twist. It is currently being used in production at StackMob.

## Usage

### sbt
```scala
"com.stackmob" %% "scaliak" % "0.4.0" 
```
### maven
```xml
<dependency>
  <groupId>com.stackmob</groupId>
  <artifactId>scaliak_${scala.version}</artifactId>
  <version>0.4.0</version>
</dependency>
```

## Status

Scaliak is currently feature incomplete vs. the original High-Level Riak Java Client. What is currently supported are mostly features being used in production (there have been a few features implemented and subsequently not used). 

The following is supported:

  - creating a client (either via raw client or w/ the http convenience method)
  - client ids (for pre riak 1.0 clusters)
  - Fetching, Updating Buckets buckets
  - Fetching, Storing and Deleting Data
  - Domain Object Conversion
  - Mutation
  - Conflict Resolution
  - Fetch by index (values only, ranges coming soon_

The following is missing:

  - retriers (coming soon)
  - Convenience methods for creating pbc or default http clients
  - ReadObject is not 1-1 with IRiakObject
  - cannot specify delete meta (coming soon)
  - link walking (WIP)
  - Fetch by index range (coming soon)
  - map reduce 
  - generalize result types to any Monad instead of `Validation`. 
  - probably more

## Design

The [High-Level Riak Java Client](https://github.com/basho/riak-java-client) developed by Basho has a great model for working with data stored in Riak. However, it can be a bit cumbersome to use from Scala where a library written for the language can provide a more concise interface. 

The Riak Java Client uses the concept of a `Converter`, `Resolver`, and `Mutation` to work with your Riak data. The `Converter` allows for conversion of domain objects and the `Resolver` provides an interface for conflict resolution in your domain. Since you don't know whether the data you are writing to data already exists, the `Mutation` provides an interface for how to insert/update data given the possibly existing data. 

Each operation in the Java client is represented by a subclass of `RiakOperation<T>`. `FetchObject`, `StoreObject` and `DeleteObject` (and a few others) implement logic for actually executing the Riak request. `DefaultBucket` is used to generate instances of the operations. Each operation has some or all of the above mentioned interfaces attached to it (fetch for example has a `Converter` and a `Resolver`). 

Scaliak follows a similar model but takes advantage of some things we can do in Scala:

- Immutability wherever possible 
- Use implicit scope and type inference to determine the converter, resolver and mutation for a particular operation
- Use the `IO` [warm fuzzy thing](http://www.urbandictionary.com/define.php?term=Warm%20Fuzzy%20Thing) to represent the equivalent of `RiakOperation<T>`
- Safe methods that throw no exceptions, all results are exposed as `scalaz.Validation[E, A]`. Exception handling for all operations is built into the `IO` representing the operation. 
- No nulls! All optional values are represented using Option[T]

Scaliak is built on top of the `RawClient` interface that underlies the High-Level Java Client. This means that you can use either the HTTP or Protobufs interface with Scaliak (although only HTTP has been tested :))

## Warm Fuzzy IO

Scaliak makes heavy use of `IO` from Scalaz to represent all calls to the `RawClient` and therefore Riak. You do not need to know how to use `IO` save one method, `unsafePerformIO`. This is the equivalent of `RiakOperation<T>`s `execute` method. If you do not call it, nothing will happen. 

Using IO does have some other benefits you can take advantage of, if you want to sequence Riak actions and perform some computations in between or as a result but this can also be done just using `unsafePerformIO`. 

All `IO` actions returned are setup to handle any exception thrown in the process, this is why they will typically have a type like `IO[Validation[E, A]]` where `E` is some exception.

## Usage

For examples please [see the `examples` package](https://github.com/stackmob/scaliak/tree/master/src/main/scala/com/stackmob/scaliak/example). More documentation and a real examples project (instead of a package) are coming soon.

## Contributing

is easy!

* Fork the code
* Make your changes
* Submit a pull request

## License

Copyright 2012-2013 StackMob

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
