

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/service")

add_library(MetaService
    meta_server.cpp
    meta_service.cpp
)


if (${MAKE_TEST} STREQUAL "OFF")
    add_executable(meta_service
        meta_service_main.cpp
    )

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(meta_service PROPERTIES ENABLE_EXPORTS 1)

    target_link_libraries(meta_service
        MetaService
        ${DORIS_LINK_LIBS}
    )

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
    install(TARGETS meta_service DESTINATION ${OUTPUT_DIR}/lib/)

#     if ("${STRIP_DEBUG_INFO}" STREQUAL "ON")
#         add_custom_command(TARGET meta_service POST_BUILD
#             COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:meta_service> $<TARGET_FILE:meta_service>.dbg
#             COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:meta_service>
#             COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:meta_service>.dbg $<TARGET_FILE:meta_service>
#             )
# 
#         install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
#         install(FILES $<TARGET_FILE:meta_service>.dbg DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
#     endif()
endif()

# vim: et tw=120 ts=4 sw=4 cc=80:
