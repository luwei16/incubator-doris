// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <CLucene.h>

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "io/fs/file_system.h"
#include "util/lock.h"

namespace doris {

namespace segment_v2 {

/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 *
 */
class CLUCENE_EXPORT DorisCompoundReader : public lucene::store::Directory {
private:
    class ReaderFileEntry;
    friend class DorisCompoundReader::ReaderFileEntry;

private:
    int32_t readBufferSize;

    // base info
    lucene::store::Directory* dir;
    lucene::store::RAMDirectory* ram_dir;
    std::string directory;
    std::string file_name;
    CL_NS(store)::IndexInput* stream;

    typedef CL_NS(util)::CLHashMap<char*, ReaderFileEntry*, CL_NS(util)::Compare::Char,
                                   CL_NS(util)::Equals::Char, CL_NS(util)::Deletor::acArray,
                                   CL_NS(util)::Deletor::Object<ReaderFileEntry> > EntriesType;

    EntriesType* entries;

    doris::Mutex _this_lock;

protected:
    /** Removes an existing file in the directory-> */
    bool doDeleteFile(const char* name);

public:
    DorisCompoundReader(lucene::store::Directory* dir, const char* name,
                        int32_t _readBufferSize = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE);

    ///Destructor - only call this if you are sure the directory
    ///is not being used anymore. Otherwise use the ref-counting
    ///facilities of _CLDECDELETE
    virtual ~DorisCompoundReader();

    // Copy compound directory file to ram directory
    void copyFile(const char* file, int64_t file_length, uint8_t* buffer, int64_t buffer_length);

    /// Get a list of strings, one for each file in the directory.
    bool list(std::vector<std::string>* names) const;

    /// Returns true iff a file with the given name exists.
    bool fileExists(const char* name) const;

    /// Returns the text name of the directory
    const char* getDirName() const; ///<returns reference

    lucene::store::Directory* getDirectory();

    /// Returns the time the named file was last modified.
    int64_t fileModified(const char* name) const;

    /// Returns the length in bytes of a file in the directory.
    int64_t fileLength(const char* name) const;

    /// Returns a stream reading an existing file.
    virtual bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                           int32_t bufferSize = -1);

    /// Renames an existing file in the directory.
    void renameFile(const char* from, const char* to);

    /** Not implemented
     * @throws UnsupportedOperationException */
    void touchFile(const char* name);

    /** Not implemented
     * @throws UnsupportedOperationException */
    virtual lucene::store::IndexOutput* createOutput(const char* name);

    ///Decrease the ref-count to the directory by one. If
    ///the object is no longer needed, then the object is
    ///removed from the directory pool.
    void close();

    std::string toString() const;

    std::string getFileName() { return file_name; }

    static const char* getClassName();
    const char* getObjectName() const;
};

} // namespace segment_v2
} // namespace doris