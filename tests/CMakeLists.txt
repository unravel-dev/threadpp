message(STATUS "Enabled threadpp tests.")

set(target_name threadpp_test)

file(GLOB_RECURSE libsrc *.h *.cpp *.hpp *.c *.cc)

add_executable(${target_name} ${libsrc})

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
target_link_libraries(${target_name} PUBLIC Threads::Threads)


target_link_libraries(${target_name} PUBLIC threadpp)

set_target_properties(${target_name} PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)



include(target_warning_support)
set_warning_level(${target_name} ultra)

enable_testing()
add_test(NAME ${target_name} COMMAND ${target_name})
