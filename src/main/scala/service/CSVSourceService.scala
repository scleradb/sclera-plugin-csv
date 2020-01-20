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

package com.scleradb.plugin.datasource.csv.service

import com.scleradb.sql.expr.{ScalValueBase, CharConst}
import com.scleradb.external.service.ExternalSourceService

import com.scleradb.plugin.datasource.csv.objects.CSVSource

/** CSV data source service */
class CSVSourceService extends ExternalSourceService {
    /** Identifier for the service */
    override val id: String = CSVSourceService.id

    /** Creates a CSVSource object given the generic parameters
      * @param params Generic parameters
      */
    override def createSource(
        params: List[ScalValueBase]
    ): CSVSource = {
        if( params.size > 4 )
            throw new IllegalArgumentException(
                "Illegal number of parameters specified for \"" + id +
                "\": " + params.size
            )

        val urlStr: String = params.lift(0) match {
            case Some(CharConst(s)) if( s != "" ) => s
            case Some(v) =>
                throw new IllegalArgumentException(
                    "Illegal file name/URL specified for \"" + id +
                    "\": " + v.repr
                )
            case None =>
                throw new IllegalArgumentException(
                    "File name/URL not specified for \"" + id + "\""
                )
        }

        val formatOpt: Option[String] = params.lift(1).map {
            case CharConst(s) => s.trim.toUpperCase
            case v =>
                throw new IllegalArgumentException(
                    "Illegal format specified for \"" + id +
                    "\": " + v.repr
                )
        }

        val isHeaderPresent: Boolean = params.lift(2).map {
            case CharConst("HEADER") => true
            case CharConst("NOHEADER") => false
            case v =>
                throw new IllegalArgumentException(
                    "Illegal header flag specified for \"" + id +
                    "\": " + v.repr
                )
        } getOrElse true

        val urlColOpt: Option[String] = params.lift(3).map {
            case CharConst(path) => path.toUpperCase
            case v =>
                throw new IllegalArgumentException(
                    "Illegal path flag specified for \"" + id +
                    "\": " + v.repr
                )
        }

        new CSVSource(id, urlStr, formatOpt, isHeaderPresent, urlColOpt)
    }
}

/** Companion object. Stores the properties */
object CSVSourceService {
    /** Identifier for the service */
    val id: String = "CSV"
}
