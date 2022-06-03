cmake_minimum_required(VERSION 3.21)
project(proj4 C)

set(CMAKE_C_STANDARD 99)

add_executable(proj4 pthread_pool.c)

# main.c