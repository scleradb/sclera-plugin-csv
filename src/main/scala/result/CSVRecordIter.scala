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

import scala.jdk.CollectionConverters._

import org.apache.commons.csv.CSVRecord
import com.scleradb.util.io.Content

/** Iterator over the CSV records across all content
  *
  * @param contentIter Iterator over the CSV content
  * @param formatOpt CSV file format
  * @param isHeaderPresent Is header present?
  */
class CSVRecordIter(
    contentIter: Iterator[Content],
    formatOpt: Option[String],
    isHeaderPresent: Boolean
) extends Iterator[(String, Iterable[String])] {
    /** Iteration state - current reader and its look-ahead, if any */
    private var stateOpt: Option[(CSVReader, Option[CSVRecord])] = None

    /** Headers of the last closed reader */
    private var prevHeadersOpt: Option[List[String]] = None

    /** Lookahead reader and record, if present */
    private def lookAheadOpt(): Option[(CSVReader, CSVRecord)] =
        stateOpt match {
            case Some((reader, Some(record))) => Some((reader, record))

            case Some((reader, None)) =>
                if( reader.recordIter.hasNext ) {
                    val record: CSVRecord = reader.recordIter.next()
                    stateOpt = Some(reader, Some(record))
                } else {
                    prevHeadersOpt = reader.headersOpt
                    reader.close()
                    stateOpt = None
                }

                lookAheadOpt()

            case None =>
                if( contentIter.hasNext ) {
                    val content: Content = contentIter.next()
                    val reader: CSVReader =
                        new CSVReader(content, formatOpt, isHeaderPresent)

                    stateOpt = Some(reader, None)

                    lookAheadOpt()
                } else None
        }

    override def hasNext: Boolean = !lookAheadOpt().isEmpty

    override def next(): (String, Iterable[String]) = lookAheadOpt() match {
        case Some((reader, record)) =>
            stateOpt = Some((reader, None))
            (reader.url, record.asScala)

        case None => Iterator.empty.next()
    }

    /** CSV header columns for the latest reader, if present */
    def headersOpt(): Option[List[String]] = lookAheadOpt() match {
        case Some((reader, _)) => reader.headersOpt
        case None => prevHeadersOpt
    }

    /** Lookahead record, if present */
    def lookAheadRecordOpt(): Option[Iterable[String]] =
        lookAheadOpt().map { case (_, record) => record.asScala }

    /** Release allocated resources */
    def close(): Unit = {
        stateOpt.foreach { case (reader, _) => reader.close() }
        stateOpt = None
    }
}
