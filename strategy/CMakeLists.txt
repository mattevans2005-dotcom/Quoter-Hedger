cmake_minimum_required(VERSION 3.16)

project(strategy)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(strategy
    quoter.cpp
)

target_link_libraries(strategy
    nats
)

