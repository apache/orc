# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.files import apply_conandata_patches, export_conandata_patches, get, copy, rmdir
from conan.tools.scm import Version

import os

required_conan_version = ">=1.54.0"

class OrcRecipe(ConanFile):
    name = "orc"
    description = "The smallest, fastest columnar storage for Hadoop workloads"
    license = "Apache-2.0"
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://orc.apache.org/"
    topics = ("orc", "columnar-storage", "hadoop")
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    @property
    def _minimum_cpp_standard(self):
        return 11 if Version(self.version) < "1.9.0" else 17

    def source(self):
        if not self.version in self.conan_data.get("sources", {}):
            import shutil
            top_level = os.environ.get("ORC_HOME")
            shutil.copytree(os.path.join(top_level, "c++"),
                            os.path.join(self.source_folder, "c++"))
            shutil.copytree(os.path.join(top_level, "cmake_modules"),
                            os.path.join(self.source_folder, "cmake_modules"))
            top_level_files = [
                "CMakeLists.txt",
                "LICENSE",
                "NOTICE",
            ]
            for top_level_file in top_level_files:
                shutil.copy(os.path.join(top_level, top_level_file),
                            self.source_folder)
            return
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def export_sources(self):
        export_conandata_patches(self)

    def requirements(self):
        self.requires("protobuf/3.19.4")
        self.requires("zlib/1.3")
        self.requires("snappy/1.1.9")
        self.requires("lz4/1.9.4")
        self.requires("zstd/1.5.5")

    def layout(self):
        cmake_layout(self, src_folder="src", build_folder="build")

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()
        tc = CMakeToolchain(self)
        tc.variables["ORC_PACKAGE_KIND"] = "conan"
        tc.variables["BUILD_JAVA"] = "OFF"
        tc.variables["BUILD_CPP_TESTS"] = "OFF"
        tc.variables["BUILD_TOOLS"] = "OFF"
        tc.variables["BUILD_LIBHDFSPP"] = "OFF"
        tc.variables["BUILD_POSITION_INDEPENDENT_LIB"] = bool(self.options.get_safe("fPIC", True))
        tc.variables["INSTALL_VENDORED_LIBS"] = "OFF"
        tc.generate()

    def build(self):
        apply_conandata_patches(self)
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, pattern="LICENSE", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        copy(self, pattern="NOTICE", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        cmake = CMake(self)
        cmake.install()
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))
        rmdir(self, os.path.join(self.package_folder, "lib", "pkgconfig"))
        rmdir(self, os.path.join(self.package_folder, "share"))

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "orc")
        self.cpp_info.set_property("cmake_target_name", "orc::orc")
        self.cpp_info.set_property("pkg_config_name", "liborc")
        self.cpp_info.libs = ["orc"]
