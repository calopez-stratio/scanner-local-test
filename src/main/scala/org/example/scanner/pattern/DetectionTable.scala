package org.example.scanner.pattern

case class DetectionTable(tablename: String) {
  def tableNameSafe: String = tablename.split('.').map(s => s"`$s`").mkString(".")
}
