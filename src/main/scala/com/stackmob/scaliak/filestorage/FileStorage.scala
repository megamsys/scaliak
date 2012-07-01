package com.stackmob.scaliak.filestorage

import scalaz._
import Scalaz._
import effects._

import java.io.{FileInputStream, File, InputStream}
import com.stackmob.scaliak.ScaliakConverter
import com.stackmob.scaliak.ScaliakPbClientPool
import com.stackmob.scaliak.ScaliakBucket
import com.stackmob.scaliak.ReadObject
import com.stackmob.scaliak.WriteObject

case class FileObject(key: String, value: Array[Byte])

abstract class FileObjectHelper(val clientPool: ScaliakPbClientPool, val proposedBucketName: Option[String] = None) {

  implicit val fileDomainConverter: ScaliakConverter[FileObject] = ScaliakConverter.newConverter[FileObject](
    (o: ReadObject) ⇒ new FileObject(o.key, o.getBytes).successNel,
    (o: FileObject) ⇒ WriteObject(o.key, o.value))

  val rawApplicationBucketName = proposedBucketName.getOrElse("file_storage")

  val rawApplicationBucket = clientPool.bucket(rawApplicationBucketName).unsafePerformIO match {
    case Success(b: ScaliakBucket) ⇒ b
    case Failure(e: Throwable) ⇒ throw e
  }

  def store(f: FileObject): IO[ValidationNEL[Throwable, Option[FileObject]]] = rawApplicationBucket.store[FileObject](f)

  def store(k: String, a: Array[Byte]): IO[ValidationNEL[Throwable, Option[FileObject]]] = store(FileObject(k, a))

  def store(k: String, f: File): IO[ValidationNEL[Throwable, Option[FileObject]]] = store(FileObject(k, f))

  def store(k: String, is: InputStream): IO[ValidationNEL[Throwable, Option[FileObject]]] = store(FileObject(k, is))

  implicit def inputStreamToByteArray(is: InputStream): Array[Byte] = {
    val tempBuffer = new Array[Byte](is.available)
    is.read(tempBuffer)
    tempBuffer
  }

  implicit def fileObjectToByteArray(file: File): Array[Byte] = inputStreamToByteArray(new FileInputStream(file))
}
