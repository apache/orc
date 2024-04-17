# Build support

The Python scripts under the folder provide capabilities for formatting code.
Make sure you've installed `clang-format-13`, `clang-tidy-13` and `clang-apply-replacements-13` and cmake could find them.
We enforce the version of tools because different versions of tools may generate different results.

## clang-format

To use `run_clang_format.py` you could act like below:

```shell
mkdir build
cd build
cmake .. -DBUILD_JAVA=OFF -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make check-format # Do checks only
make format # This would apply suggested changes, take care!
```

## clang-tidy

To use `run_clang_tidy.py` you could act like below:

```shell
mkdir build
cd build
cmake .. -DBUILD_JAVA=OFF -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make -j`nproc` # Important
make check-clang-tidy # Do checks only
make fix-clang-tidy # This would apply suggested changes, take care!
```
