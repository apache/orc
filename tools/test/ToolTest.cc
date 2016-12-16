/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "orc/orc-config.hh"
#include "orc/OrcFile.hh"
#include "ToolTest.hh"

#include "wrap/orc-proto-wrapper.hh"
#include "wrap/gtest-wrapper.h"

#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

namespace {
  const char *exampleDirectory = 0;
  const char *buildDirectory = 0;
}

GTEST_API_ int main(int argc, char **argv) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  std::cout << "ORC version: " << ORC_VERSION << "\n";
  if (argc >= 2) {
    exampleDirectory = argv[1];
  } else {
    exampleDirectory = "../examples";
  }
  if (argc >= 3) {
    buildDirectory = argv[2];
  } else {
    buildDirectory = ".";
  }
  std::cout << "example dir = " << exampleDirectory << "\n";
  if (buildDirectory) {
    std::cout << "build dir = " << buildDirectory << "\n";
  }
  testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();
  return result;
}

std::string getFileContents(const char *filename) {
  std::ifstream in(filename, std::ios::in | std::ios::binary);
  if (in) {
    std::ostringstream contents;
    contents << in.rdbuf();
    in.close();
    return contents.str();
  }
  std::cerr << "Can't read " << filename << "\n";
  exit(1);
}

/**
 * Run the given program and set the stdout and stderr parameters to
 * the output on each of the streams. The return code of the program is
 * returned as the result.
 */
int runProgram(const std::vector<std::string>& command,
               std::string &out,
               std::string &err) {

  // create temporary filenames for stdout and stderr
  std::string stdoutStr = "/tmp/orc-test-stdout-XXXXXXXX";
  std::string stderrStr = "/tmp/orc-test-stderr-XXXXXXXX";
  char *stdoutName = const_cast<char*>(stdoutStr.data());
  char *stderrName = const_cast<char*>(stderrStr.data());
  if (mkstemp(stdoutName) == -1) {
    std::cerr << "Failed to make unique name " << stdoutName
              << " - " << strerror(errno) << "\n";
    exit(1);
  }
  if (mkstemp(stderrName) == -1) {
    std::cerr << "Failed to make unique name " << stderrName
              << " - " << strerror(errno) << "\n";
    exit(1);
  }

  // flush stdout and stderr to make sure they aren't duplicated.
  // ignore errors since pipes don't support fsync
  fsync(1);
  fsync(2);

  // actuall fork
  pid_t child = fork();
  if (child == -1) {
    std::cerr << "Failed to fork - " << strerror(errno) << "\n";
    exit(1);
  } else if (child == 0) {

    // build the parameters
    std::unique_ptr<const char*> argv(new const char*[command.size() + 1]);
    for(uint64_t i=0; i < command.size(); ++i) {
      argv.get()[i] = command[i].c_str();
    }
    argv.get()[command.size()] = 0;

    // do the stdout & stderr redirection
    int stdoutFd = open(stdoutName, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (stdoutFd == -1) {
      std::cerr << "Failed to open " << stdoutName << " - "
                << strerror(errno) << "\n";
      exit(1);
    }
    if (dup2(stdoutFd, 1) == -1) {
      std::cerr << "Failed to redirect stdout - " << strerror(errno) << "\n";
      exit(1);
    }
    close(stdoutFd);
    int stderrFd = open(stderrName, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (stderrFd == -1) {
      std::cerr << "Failed to open " << stderrName << " - "
                << strerror(errno) << "\n";
      exit(1);
    }
    if (dup2(stderrFd, 2) == -1) {
      std::cerr << "Failed to redirect stderr - " << strerror(errno) << "\n";
      exit(1);
    }
    close(stderrFd);

    // run the program
    execvp(argv.get()[0], const_cast<char * const *>(argv.get()));

    // can only reach here if the exec fails
    std::cerr << "Can't run -";
    for(uint64_t i=0; i < command.size(); ++i) {
      std::cerr << " " << command[i];
    }
    std::cerr << "\n";
    std::cerr << "Exec failed with " << strerror(errno) << "\n";
    exit(1);
  }
  int status = 0;
  pid_t result = waitpid(child, &status, 0);
  if (result == -1 || !WIFEXITED(status)) {
    std::cerr << "Can't run -";
    for(uint64_t i=0; i < command.size(); ++i) {
      std::cerr << " " << command[i];
    }
    std::cerr << "\n";
    std::cerr << "stdout: " << stdoutName << ", stderr: "
              << stderrName << "\n";
    if (result == -1) {
      std::cerr << "Error: " << strerror(errno) << "\n";
    } else if (WIFSIGNALED(status)) {
      std::cerr << "Fatal signal: " << WTERMSIG(status) << "\n";
    }
    exit(1);
  }
  out = getFileContents(stdoutName);
  if (std::remove(stdoutName) != 0) {
    std::cerr << "Failed to remove " << stdoutName << "\n";
  }
  err = getFileContents(stderrName);
  if (std::remove(stderrName) != 0) {
    std::cerr << "Failed to remove " << stderrName << "\n";
  }

  return WEXITSTATUS(status);
}

/**
 * Get the name of the given example file.
 * @param name the simple name of the example file
 */
std::string findExample(const std::string &name) {
  std::string result = exampleDirectory;
  result += "/";
  result += name;
  return result;
}

/**
 * Get the name of the given executable.
 * @param name the simple name of the executable
 */
std::string findProgram(const std::string &name) {
  std::string result = buildDirectory;
  result += "/";
  result += name;
  return result;
}
