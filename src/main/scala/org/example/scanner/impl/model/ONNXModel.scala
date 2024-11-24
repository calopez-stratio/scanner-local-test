package org.example.scanner.impl.model

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtException, OrtSession}
import java.nio.LongBuffer
import java.util
import scala.io.Source
import scala.util.Try

/**
 * An implementation of TokenNameFinder that uses ONNX models.
 */
case class ONNXModel(
                      modelPath: String,
                      vocabularyPath: String,
                      ids2Labels: Map[Int, String],
                      entities: Seq[NerEntity]
                    ) {

  private val includeAttentionMask = true
  private val includeTokenTypeIds = false
  private val documentSplitSize = 250
  private val splitOverlapSize = 50
  private val INPUT_IDS = "input_ids"
  private val ATTENTION_MASK = "attention_mask"
  private val TOKEN_TYPE_IDS = "token_type_ids"

  private val session: OrtSession = {
    val env = OrtEnvironment.getEnvironment()
    val sessionOptions = new OrtSession.SessionOptions()
    env.createSession(modelPath, sessionOptions)
  }

  private val entitiesTagsMap: Map[String, String] = entities
    .map(entity => entity.beginTag -> entity.intermediateTag)
    .toMap

  private val entitiesNameMap: Map[String, Prediction.Value] = entities
    .map(entity => entity.beginTag -> entity.name)
    .toMap

  private val vocab: Map[String, Int] = loadVocab(vocabularyPath)
  private val tokenizer = NerTokenizer(vocab.keySet)
  private val env: OrtEnvironment = OrtEnvironment.getEnvironment

  def predict(input: String): Option[NerEntityResult] = {
    val wordpieceTokens = tokenize(input)
    wordpieceTokens
      .flatMap(
        nerTokens =>
          try {
            val inputs = new util.HashMap[String, OnnxTensor]()
            inputs.put(
              INPUT_IDS,
              OnnxTensor.createTensor(env, LongBuffer.wrap(nerTokens.ids.toArray), Array[Long](1, nerTokens.ids.length))
            )

            if (includeAttentionMask) {
              inputs.put(
                ATTENTION_MASK,
                OnnxTensor
                  .createTensor(env, LongBuffer.wrap(nerTokens.mask.toArray), Array[Long](1, nerTokens.mask.length))
              )
            }

            if (includeTokenTypeIds) {
              inputs.put(
                TOKEN_TYPE_IDS,
                OnnxTensor
                  .createTensor(env, LongBuffer.wrap(nerTokens.types.toArray), Array[Long](1, nerTokens.types.length))
              )
            }

            val outputValues = session.run(inputs).get(0).getValue.asInstanceOf[Array[Array[Array[Float]]]]
            val v = outputValues(0)

            val strTokens = nerTokens.tokens

            val results = v
              .zipWithIndex
              .flatMap {
                case (arr, x) =>
                  val maxIndex = arr.zipWithIndex.maxBy(_._1)._2
                  val label = ids2Labels.getOrElse(maxIndex, "0")
                  val confidence = arr(maxIndex)

                  if (entitiesTagsMap.keySet.contains(label)) {
                    val entityName = entitiesNameMap.getOrElse(label, Prediction.nullEntity)
                    val spanEnd = findSpanEnd(outputValues, x, ids2Labels, strTokens, label)

                    if (spanEnd.index != -1) { Some(NerEntityResult(entityName, confidence)) }
                    else None
                  } else None
              }
            results.headOption
          } catch { case ex: OrtException => throw new ONNXModelException("Unable to performing model inference", ex) }
      )
      .headOption
  }

  /**
   * Finds the span end based on the input parameters.
   *
   * @param v
   *   The 3D float array.
   * @param startIndex
   *   The start index in the vector.
   * @param id2Labels
   *   A map of integer labels to string values.
   * @param tokens
   *   An array of tokens.
   * @param entityBeginTag
   *   The beginning tag of the entity.
   * @return
   *   The SpanEnd instance representing the index and character end.
   */
  private def findSpanEnd(
                           v: Array[Array[Array[Float]]],
                           startIndex: Int,
                           id2Labels: Map[Int, String],
                           tokens: Seq[String],
                           entityBeginTag: String
                         ): SpanEnd = {

    var index = -1
    var characterEnd = 0

    for (x <- startIndex + 1 until v(0).length) {
      val arr = v(0)(x)
      val nextTokenClassification = id2Labels.get(arr.zipWithIndex.maxBy(_._1)._2)
      val entityIntermediateTag = entitiesTagsMap.get(entityBeginTag)

      if (!entityIntermediateTag.equals(nextTokenClassification)) {
        index = x - 1
        return SpanEnd(index, characterEnd)
      }
    }

    for (x <- 1 to index if x < tokens.length) characterEnd += tokens(x).length

    characterEnd += index - 1
    SpanEnd(index, characterEnd)
  }

  /**
   * Tokenizes the input text into smaller chunks for processing.
   *
   * @param text
   *   The input text to tokenize.
   * @return
   *   A list of Tokens representing the tokenized chunks.
   */
  private def tokenize(text: String): Seq[NerTokens] = {
    // Split the input text into individual words.
    val whitespaceTokenized = text.split("\\s+")

    // Generate chunks of words with overlap.
    val chunks = whitespaceTokenized.sliding(documentSplitSize, splitOverlapSize).toList

    // Tokenize each chunk and create Tokens objects.
    chunks.map {
      chunk =>
        val group = chunk.mkString(" ")
        val tokens = tokenizer.tokenize(group)
        val ids = tokens.map(token => vocab.getOrElse(token, -1))
        val lids = ids.map(_.toLong)
        val mask = Seq.fill(ids.length)(1L)
        val types = Seq.fill(ids.length)(0L)
        NerTokens(tokens, lids, mask, types)
    }
  }

  /**
   * Loads a vocabulary file from disk and returns a map of vocabulary words to integer IDs.
   *
   * @param vocabPath
   *   The vocabulary path to load.
   * @return
   *   A map of vocabulary words to integer IDs.
   */
  private def loadVocab(vocabPath: String): Map[String, Int] =
    Try {
      val source = Source.fromFile(vocabPath)
      val lines = source.getLines().toList
      source.close()
      lines.zipWithIndex.map { case (line, index) => line -> index }.toMap
    }.getOrElse(Map.empty[String, Int])

}

private class ONNXModelException(msg: String, thr: Throwable) extends Exception(msg, thr)
