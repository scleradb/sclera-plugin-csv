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

import java.net.URL
import java.nio.charset.Charset
import java.io.{File, BufferedReader, InputStreamReader}

import org.apache.commons.csv.{CSVFormat, CSVRecord, CSVParser}

import com.scleradb.config.ScleraConfig

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
  */
class CSVSource(
    override val name: String,
    urlStr: String,
    formatOpt: Option[String],
    isHeaderPresent: Boolean,
    val urlColOpt: Option[String]
) extends ExternalSource {
    private val format: CSVFormat = {
        val baseFormat: CSVFormat = Format(formatOpt)
        if( isHeaderPresent ) baseFormat.withHeader() else baseFormat
    }

    /** Iterator over file / directory */
    private def fileIter(f: File): Iterator[URL] = {
        if( f.isDirectory() ) {
            val ufs: List[File] =
                f.listFiles().toList.sortBy { uf => uf.getName() }
            ufs.iterator.flatMap { uf => fileIter(uf) }
        } else Iterator(f.toURI().toURL())
    }

    /** Iterator over CSV URLs */
    private def urlIter: Iterator[URL] = urlStr.split("://") match {
        case Array(path) => fileIter(new File(path))
        case Array("file", path) => fileIter(new File(path))
        case _ => Iterator(new URL(urlStr))
    }

    /** OpenCSV CSVReader object to read CSV files */
    private def csvParser(url: URL): CSVParser = CSVParser.parse(
        url,
        Charset.defaultCharset(),
        format
    )

    val headerColumns: List[Column] = {
        val urls: Iterator[URL] = urlIter
        if( !urls.hasNext ) {
            throw new IllegalArgumentException("Could not find a URL")
        }

        val reader: CSVParser = csvParser(urls.next())
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
    override val columns: List[Column] = {
        urlColOpt match {
            case Some(urlCol) =>
                Column(urlCol,  SqlCharVarying(None))::headerColumns
            case None => headerColumns
        }
    }

    /** CSVResult object to generate the result */
    override def result: CSVResult = {
        val csvReaderIter: Iterator[(String, CSVParser)] =
            urlIter.map { url => (url.toString, csvParser(url)) }

        new CSVResult(this, csvReaderIter)
    }

    /** String used for this source in the EXPLAIN output */
    override def toString: String = "%s(\"%s\")".format(name, urlStr)

    /** Serialize a proxy containing only the parameters */
    def writeReplace(): java.lang.Object =
        new SerializedCSVSource(
            name,
            urlStr,
            formatOpt,
            isHeaderPresent,
            urlColOpt
        )
}

/** Proxy object used for serialization - contains only the parameters */
class SerializedCSVSource(
    name: String,
    urlStr: String,
    formatOpt: Option[String],
    isHeaderPresent: Boolean,
    urlColOpt: Option[String]
) extends java.io.Serializable {
    /** Construct the CSV source object from the retrieved parameters */
    def readResolve(): java.lang.Object =
        new CSVSource(
            name,
            urlStr,
            formatOpt,
            isHeaderPresent,
            urlColOpt
        )
}
