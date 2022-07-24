# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test")

# where to put generated libraries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test")

add_executable(sync_point_test
   sync_point_test.cpp
)

add_executable(codec_test
   codec_test.cpp
)

add_executable(keys_test
   keys_test.cpp
)

add_executable(doris_txn_test
   doris_txn_test.cpp
)

message("Meta-service test dependencies: ${TEST_LINK_LIBS}")
target_link_libraries(sync_point_test
    ${TEST_LINK_LIBS}
)

target_link_libraries(codec_test
    ${TEST_LINK_LIBS}
)

target_link_libraries(keys_test
    ${TEST_LINK_LIBS} 
)

target_link_libraries(doris_txn_test
    ${TEST_LINK_LIBS} 
)

install(FILES
    ${BASE_DIR}/script/run_all_tests.sh
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${BUILD_DIR}/test)

# vim: et tw=120 ts=4 sw=4 cc=80:
