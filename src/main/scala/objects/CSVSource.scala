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

package com.scleradb.plugin.datasource.csv.objects

import scala.jdk.CollectionConverters._
import scala.collection.mutable

import org.apache.commons.csv.{CSVFormat, CSVRecord, CSVParser}

import com.scleradb.util.tools.{ContentIter, Content}

import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.types.SqlCharVarying

import com.scleradb.plugin.datasource.csv.result.CSVResult

import com.scleradb.external.objects.ExternalSource

/** CSV data source
  *
  * @param name Identifier of the data service - used in explain
  * @param urlStr CSV URL or file name or enclosing directory
  * @param formatOpt CSV file format (optional)
  * @param isHeaderPresent Is header present?
  * @param urlColOpt Optional column name, to contain the file name/URL
  * @param sourcePatterns List of patterns to filter source path names
  */
class CSVSource(
    override val name: String,
    urlStr: String,
    formatOpt: Option[String],
    isHeaderPresent: Boolean,
    val urlColOpt: Option[String],
    sourcePatterns: List[String]
) extends ExternalSource {
    val csvFormat: CSVFormat = {
        val baseFormat: CSVFormat = Format(formatOpt)
        if( isHeaderPresent ) baseFormat.withHeader() else baseFormat
    }

    /** CSV from text content */
    private def csvParser(text: String): CSVParser =
        CSVParser.parse(text, csvFormat)

    /** Fetches content from URL / files / directories */
    private val contentIter: ContentIter = ContentIter(sourcePatterns)

    /** Iterator over file / directory content */
    private val dataIter: Iterator[Content] = contentIter.iter(urlStr)

    /** Iterator head materialized upfront */
    private val dataHead: Content = if( dataIter.hasNext ) dataIter.next else {
        throw new IllegalArgumentException(
            s"Could not find any content at $urlStr"
        )
    }

    /** CSV header columns -- assuming all content is structured identically */
    val headerColumns: List[Column] = {
        val reader: CSVParser = csvParser(dataHead.text)

        val headersOpt: Option[mutable.Map[String, Integer]] =
            Option(reader.getHeaderMap()).map { m => m.asScala }

        val headerCols: List[Column] = headersOpt match {
            case Some(headers) =>
                headers.toList.sortBy { case (_, i) => i } map { case (v, _) =>
                    Column(v.trim.toUpperCase, SqlCharVarying(None))
                }

            case None =>
                val records: java.util.Iterator[CSVRecord] = reader.iterator()
                if( records.hasNext ) {
                    records.next().iterator().asScala.zipWithIndex.toList.map {
                        case (v, i) => Column("COL" + i, SqlCharVarying(None))
                    }
                } else List(Column("COL", SqlCharVarying(None)))
        }

        reader.close()

        headerCols
    }

    /** Columns of the result (virtual table)
      * obtained from the first line (header) of the CSV file.
      * Each column has type CHAR VARYING.
      */
    override val columns: List[Column] = urlColOpt match {
        case Some(urlCol) =>
            Column(urlCol,  SqlCharVarying(None))::headerColumns
        case None => headerColumns
    }

    /** CSVResult object to generate the result */
    override def result: CSVResult = {
        val iter: Iterator[Content] = Iterator(dataHead) ++ dataIter
        val csvReaderIter: Iterator[(String, CSVParser)] = iter.map { content =>
            (content.name, csvParser(content.text))
        }

        new CSVResult(this, csvReaderIter)
    }

    /** String used for this source in the EXPLAIN output */
    override def toString: String = "%s(\"%s\")".format(name, urlStr)

    /** Serialize a proxy containing only the parameters */
    def writeReplace(): java.lang.Object = new SerializedCSVSource(
        name, urlStr, formatOpt, isHeaderPresent, urlColOpt, sourcePatterns
    )

    /** Free resources held while iterating over the source content files */
    def close(): Unit = contentIter.close()
}

/** Proxy object used for serialization - contains only the parameters */
class SerializedCSVSource(
    name: String,
    urlStr: String,
    formatOpt: Option[String],
    isHeaderPresent: Boolean,
    urlColOpt: Option[String],
    sourcePatterns: List[String]
) extends java.io.Serializable {
    /** Construct the CSV source object from the retrieved parameters */
    def readResolve(): CSVSource = new CSVSource(
        name, urlStr, formatOpt, isHeaderPresent, urlColOpt, sourcePatterns
    )
}
