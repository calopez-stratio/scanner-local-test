package org.example.scanner.impl.model

import opennlp.tools.tokenize.WhitespaceTokenizer

/**
 * A WordpieceTokenizer is used to tokenize text into a sequence of wordpiece tokens. Wordpiece tokens are obtained by
 * splitting words into subwords based on a given vocabulary. If a word is not present in the vocabulary, it is replaced
 * by an "[UNK]" token. The tokenizer follows Scala conventions and design patterns, and provides functional
 * implementations of the required methods.
 *
 * @param vocabulary
 *   The set of wordpiece tokens in the vocabulary.
 * @param maxTokenLength
 *   The maximum allowed length for a wordpiece token.
 */
case class NerTokenizer(private val vocabulary: Set[String], private val maxTokenLength: Int = 50) {

  private val CLASSIFICATION_TOKEN = "[CLS]"
  private val SEPARATOR_TOKEN = "[SEP]"
  private val UNKNOWN_TOKEN = "[UNK]"

  // Utility method to tokenize a single word.
  private def tokenizeWord(word: String): List[String] =
    if (word.length <= maxTokenLength) {
      @annotation.tailrec
      def tokenizeSubstring(start: Int, end: Int, acc: List[String], foundSubstring: Boolean = true): List[String] =
        if (start >= end) { if (foundSubstring) acc else acc :+ UNKNOWN_TOKEN }
        else {
          val substring = if (start > 0) "##" + word.substring(start, end) else word.substring(start, end)
          if (vocabulary.contains(substring)) tokenizeSubstring(end, word.length, acc :+ substring)
          else tokenizeSubstring(start, end - 1, acc, foundSubstring = false)
        }

      tokenizeSubstring(0, word.length, List.empty[String]) match {
        case Nil => List(UNKNOWN_TOKEN) // If word can't be represented by vocabulary pieces replace with UNKNOWN_TOKEN
        case xs  => xs
      }
    } else List(UNKNOWN_TOKEN) // If the token's length is greater than the max length just add UNKNOWN_TOKEN

  /**
   * Tokenize the input text into a sequence of words.
   *
   * The function puts spaces around punctuation, splits based on whitespace, and breaks down words into vocabulary
   * pieces if possible.
   *
   * @param text
   *   the input string to be tokenized
   * @return
   *   an Array of tokens
   */
  def tokenize(text: String): Seq[String] = {
    val spacedPunctuation = text.replaceAll("\\p{Punct}+", " $0 ")
    val words = WhitespaceTokenizer.INSTANCE.tokenize(spacedPunctuation).toList
    val tokens = CLASSIFICATION_TOKEN :: words.flatMap(tokenizeWord) ::: SEPARATOR_TOKEN :: Nil
    tokens
  }

}
