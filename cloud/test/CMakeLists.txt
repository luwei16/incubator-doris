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

add_executable(txn_kv_test
   txn_kv_test.cpp
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

# FDB related tests need to be linked with libfdb_c
set(FDB_LINKER_FLAGS "-lfdb_c -L${THIRDPARTY_DIR}/lib")

target_link_libraries(doris_txn_test
    ${TEST_LINK_LIBS} 
    ${FDB_LINKER_FLAGS}
)

target_link_libraries(txn_kv_test
    ${TEST_LINK_LIBS} 
    ${FDB_LINKER_FLAGS}
)

install(FILES
    ${BASE_DIR}/script/run_all_tests.sh
    ${BASE_DIR}/conf/fdb.cluster
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${BUILD_DIR}/test)

install(FILES
    ${THIRDPARTY_DIR}/lib/ld-linux-x86-64.so.2
    ${THIRDPARTY_DIR}/lib/libc.so.6
    ${THIRDPARTY_DIR}/lib/libdl.so.2
    ${THIRDPARTY_DIR}/lib/libfdb_c.so
    ${THIRDPARTY_DIR}/lib/libgcc_s.so.1
    ${THIRDPARTY_DIR}/lib/libm.so.6
    ${THIRDPARTY_DIR}/lib/libpthread.so.0
    ${THIRDPARTY_DIR}/lib/librt.so.1
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${BUILD_DIR}/test)

# vim: et tw=120 ts=4 sw=4 cc=80:
