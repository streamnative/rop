#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# Log4j configuration file
# ROP_LOG_CONF=

# Configuration file of settings used in Rop
# ROP_CONF=

# Extra options to be passed to the jvm
ROP_MEM=${ROP_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g"}

# Garbage collection options
ROP_GC=" -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB"

# Extra options to be passed to the jvm
ROP_EXTRA_OPTS="${ROP_EXTRA_OPTS} ${ROP_MEM} ${ROP_GC} -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"

