# AI Agent Guidelines for Apache ORC

Welcome! If you are an AI coding assistant (like GitHub Copilot, Cursor, or Gemini) working on this repository, please adhere to the following guidelines.

## 1. Project Structure
Apache ORC includes both Java and C++ libraries that are completely independent of each other.
- `c++/` - the C++ reader and writer
- `java/` - the Java reader and writer
- `docker/` - docker scripts to build and test on various linuxes
- `examples/` - various ORC example files that are used to test compatibility
- `cmake_modules/` - CMake modules
- `tools/` - C++ tools for reading and inspecting ORC files
- `site/` - the website and documentation

## 2. Build Instructions
### Java
The Java project is built using Maven wrapper. Use Java 17 or higher.
```bash
cd java
./mvnw package -DskipTests
```

### C++
The C++ project uses CMake. Use CMake 3.25.0+.
```bash
mkdir build && cd build
cmake ..
make package
```
*Note: To build with Meson, refer to the README.md.*

## 3. Testing
Before submitting changes, ensure you run the appropriate test suites.

### Testing Java
```bash
cd java
./mvnw test
```

### Testing C++
```bash
cd build
make test-out
```

## 4. Code Style & Linting
- Always keep the scope of your changes minimal.
- Do not make formatting changes that are unrelated to the current task.
- Follow the existing style of the file you are modifying (e.g., C++ formatting using `clang-format`, Java formatting rules).

## 5. Submitting Changes & Bug Tracking
- **Jira**: Apache ORC uses Jira for issue tracking (https://orc.apache.org/bugs). 
- **Commit Messages**: Reference the Jira ticket in the commit message if applicable (e.g., `ORC-XXXX: Fix issue...`). There is a helper script `dev/create_orc_jira.py`.
- **Pull Requests**: Explain the reasoning behind your changes clearly. To ensure your changes are correct before submitting a PR to `apache/orc`, you can:
  1. Run the tests locally (`mvn test` or `make test`).
  2. Use the Docker scripts in `docker/` (e.g., `cd docker && ./run-all.sh local main`).
  3. **Trigger GitHub Actions**: Push your branch to your personal fork of the repository and open a Pull Request there. This will automatically run `.github/workflows/build_and_test.yml` on your own GitHub account's compute before you submit it to the upstream ASF repository.

## 6. General Advice
- **Do not introduce breaking changes** to the ORC file format serialization unless explicitly requested and discussed.
- Ensure cross-compatibility between C++ and Java implementations if you are making logic or behavioral changes to readers / writers.
