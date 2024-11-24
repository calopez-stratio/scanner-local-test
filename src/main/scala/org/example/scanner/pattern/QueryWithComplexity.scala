package org.example.scanner.pattern

import org.example.scanner.pattern.DataPatternComplexity.DSLComplexity
import org.example.scanner.sdk.traits.impl.FunctionDSL

case class QueryWithComplexity(query: FunctionDSL, complexity: DSLComplexity)
