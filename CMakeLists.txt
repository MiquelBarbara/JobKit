cmake_minimum_required(VERSION 3.21)
project(JobKit LANGUAGES CXX)

include(GNUInstallDirs)
find_package(Threads REQUIRED)

add_library(JobKit STATIC
    source/JobGraph.cpp
    source/JobGroup.cpp
    source/JobNode.cpp
    source/MetricsObserver.cpp
    source/ThreadPool.cpp
)
add_library(JobKit::JobKit ALIAS JobKit)

target_compile_features(JobKit PUBLIC cxx_std_20)
target_link_libraries(JobKit PUBLIC Threads::Threads)
target_include_directories(JobKit PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
# Allow Debug and Release archives to coexist in an installed package.
set_target_properties(JobKit PROPERTIES DEBUG_POSTFIX d)

# Embedded consumers opt in to installation; standalone builds install by default.
option(JOBKIT_INSTALL "Generate JobKit install and package export rules" ${PROJECT_IS_TOP_LEVEL})
if(JOBKIT_INSTALL)
    include(CMakePackageConfigHelpers)

    set(jobkit_package_dir "${CMAKE_INSTALL_LIBDIR}/cmake/JobKit")
    configure_package_config_file(cmake/JobKitConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/JobKitConfig.cmake"
        INSTALL_DESTINATION "${jobkit_package_dir}"
    )
    install(TARGETS JobKit EXPORT JobKitTargets
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    )
    install(DIRECTORY include/JobKit DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
        FILES_MATCHING PATTERN "*.h"
    )
    install(EXPORT JobKitTargets NAMESPACE JobKit::
        DESTINATION "${jobkit_package_dir}"
    )
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/JobKitConfig.cmake"
        DESTINATION "${jobkit_package_dir}"
    )
endif()
