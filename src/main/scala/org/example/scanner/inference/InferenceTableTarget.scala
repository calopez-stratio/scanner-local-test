package org.example.scanner.inference

case class InferenceTableTarget(tablename: String, column: String) {
  def tableNameSafe: String = tablename.split('.').map(s => s"`$s`").mkString(".")
}
