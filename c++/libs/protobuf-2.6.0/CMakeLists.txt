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

INCLUDE (CheckIncludeFileCXX)
CHECK_INCLUDE_FILE_CXX("tr1/unordered_map" HAVE_TR1)
CONFIGURE_FILE(config.h.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src
                    ${CMAKE_CURRENT_SOURCE_DIR}
                    ${CMAKE_CURRENT_BINARY_DIR}
                    ${ZLIB_INCLUDE_DIRS})

if (MSVC)
  SET (ATOMIC_INTERNALS
       "src/google/protobuf/stubs/atomicops_internals_x86_msvc.cc")
elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  SET (ATOMIC_INTERNALS
       "src/google/protobuf/stubs/atomicops_internals_x86_gcc.cc")
endif ()

add_library(protobuf STATIC
    "${ATOMIC_INTERNALS}"
    ${CMAKE_CURRENT_BINARY_DIR}/config.h
    src/google/protobuf/io/coded_stream.cc
    src/google/protobuf/stubs/common.cc
    src/google/protobuf/descriptor.cc
    src/google/protobuf/descriptor.pb.cc
    src/google/protobuf/descriptor_database.cc
    src/google/protobuf/dynamic_message.cc
    src/google/protobuf/extension_set.cc
    src/google/protobuf/extension_set_heavy.cc
    src/google/protobuf/generated_message_reflection.cc
    src/google/protobuf/generated_message_util.cc
    src/google/protobuf/io/gzip_stream.cc
    src/google/protobuf/io/strtod.cc
    src/google/protobuf/compiler/importer.cc
    src/google/protobuf/message.cc
    src/google/protobuf/message_lite.cc
    src/google/protobuf/stubs/once.cc
    src/google/protobuf/compiler/parser.cc
    src/google/protobuf/io/printer.cc
    src/google/protobuf/reflection_ops.cc
    src/google/protobuf/repeated_field.cc
    src/google/protobuf/service.cc
    src/google/protobuf/stubs/structurally_valid.cc
    src/google/protobuf/stubs/strutil.cc
    src/google/protobuf/stubs/substitute.cc
    src/google/protobuf/text_format.cc
    src/google/protobuf/io/tokenizer.cc
    src/google/protobuf/unknown_field_set.cc
    src/google/protobuf/wire_format.cc
    src/google/protobuf/wire_format_lite.cc
    src/google/protobuf/io/zero_copy_stream.cc
    src/google/protobuf/io/zero_copy_stream_impl.cc
    src/google/protobuf/io/zero_copy_stream_impl_lite.cc
    src/google/protobuf/stubs/stringprintf.cc
  )

install(TARGETS protobuf DESTINATION lib)

add_executable(protoc
    src/google/protobuf/compiler/main.cc
    src/google/protobuf/compiler/java/java_doc_comment.cc
    src/google/protobuf/compiler/code_generator.cc
    src/google/protobuf/compiler/command_line_interface.cc
    src/google/protobuf/compiler/subprocess.cc
    src/google/protobuf/compiler/zip_writer.cc
    src/google/protobuf/compiler/plugin.cc
    src/google/protobuf/compiler/plugin.pb.cc
    src/google/protobuf/compiler/cpp/cpp_enum.cc
    src/google/protobuf/compiler/cpp/cpp_enum_field.cc
    src/google/protobuf/compiler/cpp/cpp_extension.cc
    src/google/protobuf/compiler/cpp/cpp_field.cc
    src/google/protobuf/compiler/cpp/cpp_file.cc
    src/google/protobuf/compiler/cpp/cpp_generator.cc
    src/google/protobuf/compiler/cpp/cpp_helpers.cc
    src/google/protobuf/compiler/cpp/cpp_message.cc
    src/google/protobuf/compiler/cpp/cpp_message_field.cc
    src/google/protobuf/compiler/cpp/cpp_primitive_field.cc
    src/google/protobuf/compiler/cpp/cpp_service.cc
    src/google/protobuf/compiler/cpp/cpp_string_field.cc
    src/google/protobuf/compiler/java/java_context.cc
    src/google/protobuf/compiler/java/java_enum.cc
    src/google/protobuf/compiler/java/java_enum_field.cc
    src/google/protobuf/compiler/java/java_extension.cc
    src/google/protobuf/compiler/java/java_field.cc
    src/google/protobuf/compiler/java/java_file.cc
    src/google/protobuf/compiler/java/java_generator.cc
    src/google/protobuf/compiler/java/java_generator_factory.cc
    src/google/protobuf/compiler/java/java_helpers.cc
    src/google/protobuf/compiler/java/java_lazy_message_field.cc
    src/google/protobuf/compiler/java/java_message.cc
    src/google/protobuf/compiler/java/java_message_field.cc
    src/google/protobuf/compiler/java/java_name_resolver.cc
    src/google/protobuf/compiler/java/java_primitive_field.cc
    src/google/protobuf/compiler/java/java_service.cc
    src/google/protobuf/compiler/java/java_shared_code_generator.cc
    src/google/protobuf/compiler/java/java_string_field.cc
    src/google/protobuf/compiler/python/python_generator.cc
  )
   
  target_link_libraries(protoc
    protobuf
  )
