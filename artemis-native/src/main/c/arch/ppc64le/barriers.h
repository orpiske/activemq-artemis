/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ARTEMIS_NATIVE_BARRIERS_H
#define ARTEMIS_NATIVE_BARRIERS_H


/**
 * Ref: https://www.ibm.com/developerworks/systems/articles/powerpc.html
 */
#ifdef __powerpc64__
#define read_barrier()	__asm__ __volatile__ ("lwsync" : : : "memory")
#else
#define read_barrier()	__asm__ __volatile__ ("sync" : : : "memory")
#endif

#define write_barrier() __asm__ __volatile__ ("lwsync" : : : "memory")

#define mem_barrier() __asm__ __volatile__ ("lwsync" : : : "memory")
#define store_barrier()	__asm__ __volatile__("lwsync" : : : "memory")

#endif //ARTEMIS_NATIVE_BARRIERS_H
