/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This package has all the classes that deal with getting and writing
 * Blueflood specific objects to their corresponding datastores.
 *
 * The classes in this package follow these conventions:
 * <ul>
 *     <li><b>*IO.java</b>: interfaces that are generic and to be
 *     implemented by driver specific classes</li>
 *     <li><b>*RW.java</b>: interfaces and implementation classes
 *     that deal with constructing objects (reading/writing) for
 *     Blueflood services to consume. These interfaces and classes
 *     should NOT have driver specific code in it. They should
 *     contain only business logic necessary to construct Services
 *     objects. These business logic may involve doing a "join"
 *     operation on multiple tables/datastores/column families.</li>
 * </ul>
 *
 * Driver specific classes should be placed in individual sub packages
 * for each driver. For example: astyanax.* and datastax.*
 */
package com.rackspacecloud.blueflood.io;