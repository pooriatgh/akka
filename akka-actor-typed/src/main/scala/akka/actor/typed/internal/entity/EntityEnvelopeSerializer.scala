/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.entity

import akka.actor.typed.EntityEnvelope
import akka.actor.typed.internal.protobuf.EntityMessages
import akka.annotation.InternalApi
import akka.event.Logging
import akka.protobufv3.internal.UnsafeByteOperations
import akka.serialization.BaseSerializer
import akka.serialization.DisabledJavaSerializer
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.serialization.Serializers
import akka.util.ByteString
import akka.util.ByteString.ByteString1
import akka.util.ByteString.ByteString1C
import akka.protobufv3.internal.{ ByteString => ProtoByteString }

/**
 * INTERNAL API
 *
 * Serializer for [[EntityEnvelope]].
 */
@InternalApi private[akka] final class EntityEnvelopeSerializer(val system: akka.actor.ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private val log = Logging(system, classOf[EntityEnvelopeSerializer])
  private lazy val serialization = SerializationExtension(system)

  private final val EntityEnvelopeManifest = "a"

  override def manifest(o: AnyRef): String =
    o match {
      case _: EntityEnvelope[_] => EntityEnvelopeManifest
      case _ =>
        throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
    }

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case e: EntityEnvelope[_] =>
        EntityMessages.EntityEnvelope
          .newBuilder()
          .setEntityId(e.entityId)
          .setMessage(payloadBuilder(e.message))
          .build()
          .toByteArray

      case _ =>
        throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
    }
  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case EntityEnvelopeManifest =>
        val envelope = EntityMessages.EntityEnvelope.parseFrom(bytes)
        val payload = deserializePayload(envelope.getMessage)
        EntityEnvelope(envelope.getEntityId, payload)
    }

  private def payloadBuilder(input: Any): EntityMessages.Payload.Builder = {
    val payload = input.asInstanceOf[AnyRef]
    val builder = EntityMessages.Payload.newBuilder()
    val serializer = serialization.findSerializerFor(payload)

    payload match {
      case t: Throwable if serializer.isInstanceOf[DisabledJavaSerializer] =>
        val originalClassName = t.getClass.getName
        val originalMessage = t.getMessage
        val notSerializableException = {
          // FIXME: should be ThrowableNotSerializableException from akka.remote
          new IllegalArgumentException(s"Serialization of [$originalClassName] failed. $originalMessage", t.getCause)
        }
        log.debug(
          "Couldn't serialize [{}] because Java serialization is disabled. Fallback to " +
          "ThrowableNotSerializableException. {}",
          originalClassName,
          originalMessage)
        val serializer2 = serialization.findSerializerFor(notSerializableException)
        builder
          .setEnclosedMessage(ByteStringUtils.toProtoByteStringUnsafe(serializer2.toBinary(notSerializableException)))
          .setSerializerId(serializer2.identifier)
        val manifest = Serializers.manifestFor(serializer2, notSerializableException)
        if (manifest.nonEmpty) builder.setMessageManifest(ProtoByteString.copyFromUtf8(manifest))

      case _ =>
        builder
          .setEnclosedMessage(ByteStringUtils.toProtoByteStringUnsafe(serializer.toBinary(payload)))
          .setSerializerId(serializer.identifier)
        val manifest = Serializers.manifestFor(serializer, payload)
        if (manifest.nonEmpty) builder.setMessageManifest(ProtoByteString.copyFromUtf8(manifest))
    }

    builder
  }

  def deserializePayload(payload: EntityMessages.Payload): Any = {
    val manifest = if (payload.hasMessageManifest) payload.getMessageManifest.toStringUtf8 else ""
    serialization.deserialize(payload.getEnclosedMessage.toByteArray, payload.getSerializerId, manifest).get
  }
}

/**
 * INTERNAL API
 * FIXME: should move from akka.remote to akka.util in akka-actor
 */
@InternalApi
private[akka] object ByteStringUtils {
  def toProtoByteStringUnsafe(bytes: ByteString): ProtoByteString = {
    if (bytes.isEmpty)
      ProtoByteString.EMPTY
    else if (bytes.isInstanceOf[ByteString1C] || (bytes.isInstanceOf[ByteString1] && bytes.isCompact)) {
      UnsafeByteOperations.unsafeWrap(bytes.toArrayUnsafe())
    } else {
      // zero copy, reuse the same underlying byte arrays
      bytes.asByteBuffers.foldLeft(ProtoByteString.EMPTY) { (acc, byteBuffer) =>
        acc.concat(UnsafeByteOperations.unsafeWrap(byteBuffer))
      }
    }
  }

  def toProtoByteStringUnsafe(bytes: Array[Byte]): ProtoByteString = {
    if (bytes.isEmpty)
      ProtoByteString.EMPTY
    else {
      UnsafeByteOperations.unsafeWrap(bytes)
    }
  }
}
