/**
* Sclera - CSV
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.plugin.datasource.csv.result

import java.nio.charset.Charset
import scala.jdk.CollectionConverters._
import org.apache.commons.csv.{CSVFormat, CSVRecord, CSVParser}

import com.scleradb.util.io.Content

/** Encapsulates a CSV Reader
  *
  * @param content CSV content
  * @param formatOpt CSV file format
  * @param isHeaderPresent Is header present?
  * @param charSet Charset needed for parsing the content into CSV records
  */
class CSVReader(
    content: Content,
    formatOpt: Option[String],
    isHeaderPresent: Boolean,
    charSet: Charset = Charset.defaultCharset()
) {
    /** URL of the CSV data */
    val url: String = content.name

    /** Parser for the CSV data */
    private val parser = CSVParser.parse(
        content.inputStream, charSet,
        csvFormat(formatOpt getOrElse "Default", isHeaderPresent)
    )

    /** Iterator over records of the given CSV data */
    lazy val recordIter: Iterator[CSVRecord] = parser.iterator().asScala

    /** Header information */
    lazy val headersOpt: Option[List[String]] =
        Option(parser.getHeaderNames()).map { headers =>
            headers.asScala.toList.map { s => s.trim.toUpperCase }
        }

    /** Release allocated resources */
    def close(): Unit = parser.close()

    /** Computes CSV format from the specified configuration */
    private def csvFormat(
        format: String,
        isHeaderPresent: Boolean
    ): CSVFormat = {
        val baseFormat: CSVFormat = CSVFormat.valueOf(format)
        if( isHeaderPresent ) baseFormat.withHeader() else baseFormat
    }
}
