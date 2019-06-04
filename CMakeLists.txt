cmake_minimum_required(VERSION 3.12)
project(xredis)
add_definitions(-std=c++11)
set(CMAKE_C_STANDARD 99)

# platform differences
if(CMAKE_SYSTEM_NAME MATCHES "Linux")
        add_definitions(-D PLATFORM_LINUX)
endif(CMAKE_SYSTEM_NAME MATCHES "Linux")

if(WIN32)
        add_definitions(-D PLATFORM_WINDOWS)
endif(WIN32)

if(UNIX)
        add_definitions(-D PLATFORM_APPLE)
endif(UNIX)


# add include dirs
include_directories(
        hiredis-0.13
        src
)

# add link dirs
link_directories(/usr/local/protobuf/lib)
link_directories(/usr/local/lib)


# add cxx files
set(
        SRC_FILES       
        src/RedisConn.cpp
        src/RedisReply.cpp
 )

 # link all
add_executable(xredis ${SRC_FILES})
target_link_libraries(xredis hiredis)
