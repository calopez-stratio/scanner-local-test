package org.example.scanner.impl.model

case class NerTokens(tokens: Seq[String], ids: Seq[Long], mask: Seq[Long], types: Seq[Long])
