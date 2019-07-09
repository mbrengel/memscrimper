cmake_minimum_required(VERSION 3.10)
project(memscrimper)

if(STATIC_BUILD)
    message("-- Building statically-linked binary")
    set(Boost_USE_STATIC_LIBS ON)
endif()
if(NOT STATIC_BUILD)
    message("-- Building dynamically-linked binary")
endif()

find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost COMPONENTS system log log_setup thread iostreams REQUIRED)

set(default_build_type "Release")
# check if build type was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
    # overwrite empty build type
    set(CMAKE_BUILD_TYPE "${default_build_type}")
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wextra -pedantic")
set(CMAKE_CXX_FLAGS_RELEASE  "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DDEBUGMODE=0")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -DDEBUGMODE=1")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUGMODE=1")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize-recover=address")

add_executable(memscrimper
  src/memscrimper.cpp src/memdump.cpp src/memdump.h src/interdedup_compress.cpp
  src/interdedup_compress.h src/utils.cpp src/utils.h src/interdedup_decompress.cpp
  src/interdedup_decompress.h src/socket_api.cpp src/socket_api.h src/request_handler.cpp
  src/request_handler.h)

if(STATIC_BUILD)
    find_library(ZLIB_STATIC libz.a)
    find_library(BZIP2_STATIC libbz2.a)
    target_link_libraries(memscrimper -static)
    target_link_libraries(memscrimper ${Boost_LIBRARIES} ${BZIP2_STATIC} ${ZLIB_STATIC}
                          ${CMAKE_THREAD_LIBS_INIT})
endif()
if(NOT STATIC_BUILD)
    add_definitions("-DBOOST_LOG_DYN_LINK")
    target_link_libraries(memscrimper ${Boost_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
endif()
