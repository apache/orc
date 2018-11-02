# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(NO_DOWNLOAD_MSG
  --batch-mode
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn)

# set the version in the POM file to match the CMake version string
execute_process(COMMAND mvn versions:set -DnewVersion=${ORC_VERSION}
                                         -DgenerateBackupPoms=false
                                         ${NO_DOWNLOAD_MSG}
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(ORC_JARS
  ${CMAKE_CURRENT_BINARY_DIR}/shims/orc-shims-${ORC_VERSION}.jar
  ${CMAKE_CURRENT_BINARY_DIR}/core/orc-core-${ORC_VERSION}.jar
  ${CMAKE_CURRENT_BINARY_DIR}/mapreduce/orc-mapreduce-${ORC_VERSION}.jar
  ${CMAKE_CURRENT_BINARY_DIR}/tools/orc-tools-${ORC_VERSION}-uber.jar
)

if (ANALYZE_JAVA)
  set(JAVA_PROFILE "-Pcmake,analyze,benchmark")
else()
  set(JAVA_PROFILE "-Pcmake")
endif()

add_custom_command(
   OUTPUT ${ORC_JARS}
   COMMAND mvn ${NO_DOWNLOAD_MSG} ${JAVA_PROFILE}
             -Dbuild.dir=${CMAKE_CURRENT_BINARY_DIR} -DskipTests package
   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
   COMMENT "Build the java directory"
   VERBATIM)

add_custom_target(java_build ALL DEPENDS ${ORC_JARS})

add_test(
  NAME java-test
  COMMAND mvn ${NO_DOWNLOAD_MSG} -Pcmake
           -Dbuild.dir=${CMAKE_CURRENT_BINARY_DIR} test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

install(
  FILES ${ORC_JARS}
  DESTINATION share)
