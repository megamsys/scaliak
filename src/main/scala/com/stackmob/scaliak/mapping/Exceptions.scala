/**
 * Copyright 2012-2013 StackMob
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stackmob.scaliak.mapping

sealed abstract class ScaliakMappingError[T](val propName: String, val value: T)
  extends Throwable("%s with value %s was not valid".format(propName, value.toString))

case class MappingError[T](override val propName: String, override val value: T) extends ScaliakMappingError[T](propName, value)
case class MetadataMappingError(key: String, override val value: String) extends ScaliakMappingError[String]("metadata", value)
case class MissingMetadataMappingError(key: String) extends ScaliakMappingError[String]("metadata", """"missing key: %s"""" format key)
