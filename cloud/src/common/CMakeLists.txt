# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/common")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/common")

add_library(Common
    configbase.cpp
    sync_point.cpp
)
# vim: et tw=120 ts=4 sw=4 cc=80:
