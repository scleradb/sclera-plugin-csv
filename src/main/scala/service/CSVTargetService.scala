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

import com.scleradb.external.service.ExternalTargetService

import com.scleradb.plugin.datasource.csv.objects.CSVTarget

/** CSV data target service */
class CSVTargetService extends ExternalTargetService {
    /** Identifier for the service */
    override val id: String = CSVTargetService.id

    /** Creates a CSVTarget object given the generic parameters
      * @param params Generic parameters
      */
    override def createTarget(
        params: List[ScalValueBase]
    ): CSVTarget = {
        if( params.size > 2 )
            throw new IllegalArgumentException(
                "Illegal number of parameters specified for \"" + id +
                "\": " + params.size
            )

        val fileName: String = params.lift(0) match {
            case Some(CharConst(s)) if( s != "" ) => s
            case Some(v) =>
                throw new IllegalArgumentException(
                    "Illegal file name specified for \"" + id +
                    "\": " + v.repr
                )
            case None =>
                throw new IllegalArgumentException(
                    "File name not specified for \"" + id + "\""
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

        new CSVTarget(
            id,
            fileName,
            formatOpt
        )
    }
}

/** Companion object. Stores the properties */
object CSVTargetService {
    /** Identifier for the service */
    val id: String = "CSV"
}
