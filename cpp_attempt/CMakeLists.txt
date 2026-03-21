cmake_minimum_required(VERSION 3.15)
project(gbam_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -fno-omit-frame-pointer")
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Allow local installs (e.g. ~/.local)
list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local")
include_directories("$ENV{HOME}/.local/include")
link_directories("$ENV{HOME}/.local/lib")

# nlohmann/json (header-only, bundled in third_party/)
include_directories(${CMAKE_SOURCE_DIR}/third_party)

find_package(OpenMP REQUIRED)

add_executable(gbam
    main.cpp
    writer.cpp
    reader.cpp
    codec.cpp
)

# Link brotli statically to avoid shared library symbol resolution issues
find_library(BROTLI_DEC_LIB    NAMES libbrotlidec-static.a    libbrotlidec.a    brotlidec    PATHS "$ENV{HOME}/.local/lib" NO_DEFAULT_PATH)
find_library(BROTLI_ENC_LIB    NAMES libbrotlienc-static.a    libbrotlienc.a    brotlienc    PATHS "$ENV{HOME}/.local/lib" NO_DEFAULT_PATH)
find_library(BROTLI_COMMON_LIB NAMES libbrotlicommon-static.a libbrotlicommon.a brotlicommon PATHS "$ENV{HOME}/.local/lib" NO_DEFAULT_PATH)

target_link_libraries(gbam
    hts
    z
    lz4
    ${BROTLI_DEC_LIB}
    ${BROTLI_ENC_LIB}
    ${BROTLI_COMMON_LIB}
    zstd
    OpenMP::OpenMP_CXX
)
