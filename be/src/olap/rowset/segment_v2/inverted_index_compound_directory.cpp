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

#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"

#include <fcntl.h>

#include "cloud/io/file_reader.h"
#include "cloud/io/file_writer.h"
#include "util/md5.h"

// #include <CLucene/_ApiHeader.h>
#include "CLucene/SharedHeader.h"
#include "CLucene/StdHeader.h"

#ifdef _CL_HAVE_IO_H
#include <io.h>
#endif
#ifdef _CL_HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef _CL_HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef _CL_HAVE_DIRECT_H
#include <direct.h>
#endif
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/IndexWriter.h>
#include <CLucene/util/Misc.h>
#include <assert.h>
#include <errno.h>
// #include <CLucene/util/_MD5Digester.h>
#include <CLucene/store/LockFactory.h>

#ifdef LUCENE_FS_MMAP
#include "_MMapIndexInput.h"
#endif

#define CL_MAX_PATH 4096
//this is the max filename... for now its just the same,
//but this could change, so we use a different name
#define CL_MAX_NAME CL_MAX_PATH
//this used to be CL_MAX_NAME * 32, but as Alex Hudson points out, this could come to be 128kb.
//the above logic for CL_MAX_NAME should be correct enough to handle all file names
#define CL_MAX_DIR CL_MAX_PATH

#if defined(_WIN32) || defined(_WIN64)
#define PATH_DELIMITERA "\\"
#else
#define PATH_DELIMITERA "/"
#endif

namespace doris {
namespace segment_v2 {

const char* WRITE_LOCK_FILE = "write.lock";
const char* COMPOUND_FILE_EXTENSION = ".idx";
const int64_t MAX_HEADER_DATA_SIZE = 1024 * 128; // 128k

/** This cache of directories ensures that there is a unique Directory
   * instance per path, so that synchronization on the Directory can be used to
   * synchronize access between readers and writers.
   */
//static CL_NS(util)::CLHashMap<const char*, DorisCompoundDirectory*, CL_NS(util)::Compare::Char,
//                              CL_NS(util)::Equals::Char> DIRECTORIES(false, false);
//STATIC_DEFINE_MUTEX(DIRECTORIES_LOCK)
//static doris::Mutex _DIRECTORIES_LOCK;

bool DorisCompoundDirectory::disableLocks = false;

/**
 * Lucene compound file format
 *
 * <p>Files:
 *
 * <ul>
 *   <li><code>.idx</code>: An "virtual" file consisting of all the other index files for
 *       systems that frequently run out of file handles.
 * </ul>
 *
 * <p>Description:
 *
 * <ul>
 *   <li>Compound (.idx) --&gt; FileCount, &lt;FileName, DataOffset,
 *       DataLength, [FileData]<sup>HeaderFileCount</sup>&gt; <sup>FileCount</sup>,
 *       FileData <sup>FileCount-HeaderFileCount</sup>
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>HeaderFileCount indicates how many files' are contained in compound file header. The max
 *       data size in header is at most 128k.
 *   <li>FileCount indicates how many files are contained in this compound file. The entry table
 *       that follows has that many entries.
 *   <li>Each directory entry contains a long pointer to the start of this file's data section, the
 *       files length, and a String with that file's name.
 * </ul>
 * @param dir
 */
DorisCompoundFileWriter::DorisCompoundFileWriter(CL_NS(store)::Directory* dir) {
    if (dir == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "directory cannot be null");
    }

    directory = dir;
}

CL_NS(store)::Directory* DorisCompoundFileWriter::getDirectory() {
    return directory;
}

void DorisCompoundFileWriter::writeCompoundFile() {
    // list files in current dir
    std::vector<std::string> files;
    directory->list(&files);
    // remove write.lock file
    auto it = std::find(files.begin(), files.end(), WRITE_LOCK_FILE);
    if (it != files.end()) {
        files.erase(it);
    }
    // sort file list by file length
    std::vector<std::pair<std::string, int64_t>> sorted_files;
    for (auto file : files) {
        sorted_files.push_back(std::make_pair(
                file, ((DorisCompoundDirectory*)directory)->fileLength(file.c_str())));
    }
    std::sort(sorted_files.begin(), sorted_files.end(),
              [](const std::pair<std::string, int64_t>& a,
                 const std::pair<std::string, int64_t>& b) { return (a.second < b.second); });

    int32_t file_count = sorted_files.size();

    // get index file name
    //io::Path path(((DorisCompoundDirectory*)directory)->getDirName());
    //auto idx_path = path.parent_path();
    //std::string idx_name = std::string(path.stem().c_str()) + COMPOUND_FILE_EXTENSION;

    io::Path cfs_path(((DorisCompoundDirectory*)directory)->getCfsDirName());
    auto idx_path = cfs_path.parent_path();
    std::string idx_name = std::string(cfs_path.stem().c_str()) + COMPOUND_FILE_EXTENSION;
    // write file entries to ram directory to get header length
    lucene::store::RAMDirectory ram_dir;
    auto out_idx = ram_dir.createOutput(idx_name.c_str());
    if (out_idx == nullptr) {
        LOG(WARNING) << "Write compound file error: RAMDirectory output is nullptr.";
        return;
    }

    std::unique_ptr<lucene::store::IndexOutput> ram_output(out_idx);
    ram_output->writeVInt(file_count);
    // write file entries in ram directory
    // number of files, which data are in header
    int header_file_count = 0;
    int64_t header_file_length = 0;
    const int64_t buffer_length = 16384;
    uint8_t ram_buffer[buffer_length];
    for (auto file : sorted_files) {
        ram_output->writeString(file.first); // file name
        ram_output->writeLong(0);            // data offset
        ram_output->writeLong(file.second);  // file length
        header_file_length += file.second;
        if (header_file_length <= MAX_HEADER_DATA_SIZE) {
            copyFile(file.first.c_str(), ram_output.get(), ram_buffer, buffer_length);
            header_file_count++;
        }
    }
    auto header_len = ram_output->getFilePointer();
    ram_output->close();
    ram_dir.deleteFile(idx_name.c_str());
    ram_dir.close();

    // write real file
    // rowset data file home
    // lock problem?
    auto compound_fs = ((DorisCompoundDirectory*)directory)->getCompoundFileSystem();
    auto out_dir = DorisCompoundDirectory::getDirectory(compound_fs, idx_path.c_str(), false);

    auto out = out_dir->createOutput(idx_name.c_str());
    if (out == nullptr) {
        LOG(WARNING) << "Write compound file error: CompoundDirectory output is nullptr.";
        return;
    }
    std::unique_ptr<lucene::store::IndexOutput> output(out);
    output->writeVInt(file_count);
    // write file entries
    int64_t data_offset = header_len;
    uint8_t header_buffer[buffer_length];
    for (int i = 0; i < sorted_files.size(); ++i) {
        auto file = sorted_files[i];
        output->writeString(file.first); // FileName
        // DataOffset
        if (i < header_file_count) {
            // file data write in header, so we set its offset to -1.
            output->writeLong(-1);
        } else {
            output->writeLong(data_offset);
        }
        output->writeLong(file.second); // FileLength
        if (i < header_file_count) {
            // append data
            copyFile(file.first.c_str(), output.get(), header_buffer, buffer_length);
        } else {
            data_offset += file.second;
        }
    }
    // write rest files' data
    uint8_t data_buffer[buffer_length];
    for (int i = header_file_count; i < sorted_files.size(); ++i) {
        auto file = sorted_files[i];
        copyFile(file.first.c_str(), output.get(), data_buffer, buffer_length);
    }
    out_dir->close();
    // NOTE: need to decrease ref count, but not to delete here,
    // because index cache may get the same directory from DIRECTORIES
    _CLDECDELETE(out_dir);
    output->close();
}

void DorisCompoundFileWriter::copyFile(const char* fileName, lucene::store::IndexOutput* output,
                                       uint8_t* buffer, int64_t bufferLength) {
    lucene::store::IndexInput* tmp = nullptr;
    CLuceneError err;
    if (!directory->openInput(fileName, tmp, err)) {
        throw err;
    }

    std::unique_ptr<lucene::store::IndexInput> input(tmp);
    int64_t start_ptr = output->getFilePointer();
    int64_t length = input->length();
    int64_t remainder = length;
    int64_t chunk = bufferLength;

    while (remainder > 0) {
        int64_t len = std::min(std::min(chunk, length), remainder);
        input->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        TCHAR buf[CL_MAX_PATH + 100];
        swprintf(buf, CL_MAX_PATH + 100,
                 _T("Non-zero remainder length after copying")
                 _T(": %d (id: %s, length: %d, buffer size: %d)"),
                 (int)remainder, fileName, (int)length, (int)chunk);
        _CLTHROWT(CL_ERR_IO, buf);
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != length) {
        TCHAR buf[100];
        swprintf(buf, 100,
                 _T("Difference in the output file offsets %d ")
                 _T("does not match the original file length %d"),
                 (int)diff, (int)length);
        _CLTHROWA(CL_ERR_IO, buf);
    }
    input->close();
}

class DorisCompoundDirectory::FSIndexOutput : public lucene::store::BufferedIndexOutput {
private:
    // int32_t fhandle;
    io::FileWriterPtr writer;

protected:
    // output methods:
    void flushBuffer(const uint8_t* b, const int32_t size) override;

public:
    FSIndexOutput() = default;
    void init(const io::FileSystemSPtr& fileSystem, const char* path);
    ~FSIndexOutput() override;

    // output methods:
    void close() override;

    // Random-access methods
    void seek(const int64_t pos) override;
    int64_t length() const override;
};

bool DorisCompoundDirectory::FSIndexInput::open(const io::FileSystemSPtr& fs, const char* path,
                                                IndexInput*& ret, CLuceneError& error,
                                                int32_t __bufferSize) {
    //Func - Constructor.
    //       Opens the file named path
    //Pre  - path != NULL
    //Post - if the file could not be opened  an exception is thrown.

    CND_PRECONDITION(path != NULL, "path is NULL");

    if (__bufferSize == -1) __bufferSize = CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE;
    SharedHandle* handle = _CLNEW SharedHandle(path);

    //Open the file
    // handle->fhandle = ::_cl_open(path, _O_BINARY | O_RDONLY | _O_RANDOM, _S_IREAD);
    if (!fs->open_file(path, &handle->reader).ok()) {
        error.set(CL_ERR_IO, "open file error");
    }

    //Check if a valid handle was retrieved
    if (handle->reader) {
        //Store the file length
        handle->_length = handle->reader->size();
        // if (handle->_length == -1)
        if (handle->reader->size() < 0)
            error.set(CL_ERR_IO, "fileStat error");
        else {
            handle->_fpos = 0;
            ret = _CLNEW FSIndexInput(handle, __bufferSize);
            return true;
        }
    } else {
        int err = errno;
        if (err == ENOENT)
            error.set(CL_ERR_IO, "File does not exist");
        else if (err == EACCES)
            error.set(CL_ERR_IO, "File Access denied");
        else if (err == EMFILE)
            error.set(CL_ERR_IO, "Too many open files");
        else
            error.set(CL_ERR_IO, "Could not open file");
    }
#ifndef _CL_DISABLE_MULTITHREADING
    //delete handle->SHARED_LOCK;
#endif
    _CLDECDELETE(handle);
    return false;
}

DorisCompoundDirectory::FSIndexInput::FSIndexInput(const FSIndexInput& other)
        : BufferedIndexInput(other) {
    //Func - Constructor
    //       Uses clone for its initialization
    //Pre  - clone is a valide instance of FSIndexInput
    //Post - The instance has been created and initialized by clone
    if (other.handle == NULL) _CLTHROWA(CL_ERR_NullPointer, "other handle is null");

    // SCOPED_LOCK_MUTEX(*other.handle->SHARED_LOCK)
    std::lock_guard<doris::Mutex> wlock(other.handle->_shared_lock);
    handle = _CL_POINTER(other.handle);
    _pos = other.handle->_fpos; //note where we are currently...
}

DorisCompoundDirectory::FSIndexInput::SharedHandle::SharedHandle(const char* path) {
    // fhandle = 0;
    // reader = nullptr;
    _length = 0;
    _fpos = 0;
    strcpy(this->path, path);

#ifndef _CL_DISABLE_MULTITHREADING
    //SHARED_LOCK = new _LUCENE_THREADMUTEX;
#endif
}

DorisCompoundDirectory::FSIndexInput::SharedHandle::~SharedHandle() {
    if (reader) {
        if (reader->close().ok()) reader = nullptr;
    }
}

DorisCompoundDirectory::FSIndexInput::~FSIndexInput() {
    //Func - Destructor
    //Pre  - True
    //Post - The file for which this instance is responsible has been closed.
    //       The instance has been destroyed

    FSIndexInput::close();
}

lucene::store::IndexInput* DorisCompoundDirectory::FSIndexInput::clone() const {
    return _CLNEW DorisCompoundDirectory::FSIndexInput(*this);
}
void DorisCompoundDirectory::FSIndexInput::close() {
    BufferedIndexInput::close();
#ifndef _CL_DISABLE_MULTITHREADING
    if (handle != NULL) {
        //here we have a bit of a problem... we need to lock the handle to ensure that we can
        //safely delete the handle... but if we delete the handle, then the scoped unlock,
        //won't be able to unlock the mutex...

        //take a reference of the lock object...
        //_LUCENE_THREADMUTEX* mutex = handle->SHARED_LOCK;
        //lock the mutex
        handle->_shared_lock.lock();

        //determine if we are about to delete the handle...
        bool dounlock = (_LUCENE_ATOMIC_INT_GET(handle->__cl_refcount) > 1);

        //decdelete (deletes if refcount is down to 0
        _CLDECDELETE(handle);

        //printf("handle=%d\n", handle->__cl_refcount);
        if (dounlock) {
            handle->_shared_lock.unlock();
        }
    }
#else
    _CLDECDELETE(handle);
#endif
}

void DorisCompoundDirectory::FSIndexInput::seekInternal(const int64_t position) {
    CND_PRECONDITION(position >= 0 && position < handle->_length, "Seeking out of range");
    _pos = position;
}

/** IndexInput methods */
void DorisCompoundDirectory::FSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    CND_PRECONDITION(handle != NULL, "shared file handle has closed");
    // CND_PRECONDITION(handle->fhandle >= 0, "file is not open");
    CND_PRECONDITION(handle->reader != nullptr, "file is not open");
    // SCOPED_LOCK_MUTEX(*handle->SHARED_LOCK)
    std::lock_guard<doris::Mutex> wlock(handle->_shared_lock);

    if (handle->_fpos != _pos) {
        // if (fileSeek(handle->fhandle, _pos, SEEK_SET) != _pos) {
        //     _CLTHROWA(CL_ERR_IO, "File IO Seek error");
        // }
        handle->_fpos = _pos;
    }

    io::IOState state;
    Slice result {b, (size_t)len};
    size_t bytes_read = 0;
    if (!handle->reader->read_at(_pos, result, &bytes_read, &state).ok()) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }
    bufferLength = len; // TODO xk read should return size
    if (bytes_read != len) {
        //if (EINTR == errno) we could do something else... but we have
        //to guarantee some return, or throw EOF

        _CLTHROWA(CL_ERR_IO, "read error");
    }
    _pos += bufferLength;
    handle->_fpos = _pos;
}

void DorisCompoundDirectory::FSIndexOutput::init(const io::FileSystemSPtr& fileSystem, const char* path) {
    // create file path
    Status status = fileSystem->create_file(path, &writer);
    if (!status.ok()) {
        // create file failed, set writer to nullptr
        writer.reset(nullptr);
        auto err = "Create compound file error: " + status.get_error_msg();
        LOG(WARNING) << err;
        _CLTHROWA(CL_ERR_IO, err.c_str());
    }
}

DorisCompoundDirectory::FSIndexOutput::~FSIndexOutput() {
    if (writer) {
        try {
            FSIndexOutput::close();
        } catch (CLuceneError& err) {
            //ignore errors...
            LOG(WARNING) << "FSIndexOutput deconstruct error: " << err.what();
        }
    }
}

/** output methods: */
void DorisCompoundDirectory::FSIndexOutput::flushBuffer(const uint8_t* b, const int32_t size) {
    // CND_PRECONDITION(fhandle >= 0, "file is not open");
    // if (size > 0 && _write(fhandle, b, size) != size) _CLTHROWA(CL_ERR_IO, "File IO Write error");
    //CND_PRECONDITION(writer != nullptr, "file is not open");
    // insure file_writer is not nullptr
    if (writer != nullptr && b != nullptr && size > 0) {
        Slice data {b, (size_t)size};
        Status st = writer->append(data);
        if (!st.ok()) {
            LOG(WARNING) << "File IO Write error: " << st.get_error_msg();
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore flush.";
    }
}

void DorisCompoundDirectory::FSIndexOutput::close() {
    try {
        BufferedIndexOutput::close();
    } catch (CLuceneError& err) {
        //ignore IO errors...
        LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close error: " << err.what();
        if (err.number() != CL_ERR_IO) {
            LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close IO error: " << err.what();
            throw;
        }
    }
    if (writer) {
        Status ret = writer->finalize();
        if (ret != Status::OK()) {
            LOG(WARNING) << "FSIndexOutput close, file writer finalize error: " << ret.to_string();
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
        ret = writer->close();
        if (ret != Status::OK()) {
            LOG(WARNING) << "FSIndexOutput close, file writer close error: " << ret.to_string();
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore finalize and close.";
    }
    writer = nullptr;
}

void DorisCompoundDirectory::FSIndexOutput::seek(const int64_t pos) {
    // CND_PRECONDITION(fhandle >= 0, "file is not open");
    CND_PRECONDITION(writer != nullptr, "file is not open");
    BufferedIndexOutput::seek(pos);
    // int64_t ret = fileSeek(fhandle, pos, SEEK_SET);
    int64_t ret = pos; // TODO xk
    if (ret != pos) {
        _CLTHROWA(CL_ERR_IO, "File IO Seek error");
    }
}

int64_t DorisCompoundDirectory::FSIndexOutput::length() const {
    // CND_PRECONDITION(fhandle >= 0, "file is not open");
    // return fileSize(fhandle);
    CND_PRECONDITION(writer != nullptr, "file is not open");
    // return writer->bytes_appended(); // TODO xk

    struct stat64 buf;
    char buffer[CL_MAX_DIR];
    buffer[0] = 0;
    strcat(buffer, writer->path().c_str());
    if (stat64(buffer, &buf) == -1) // TODO xk
        return 0;
    else
        return buf.st_size;
}

const char* DorisCompoundDirectory::LOCK_DIR = NULL;
const char* DorisCompoundDirectory::getLockDir() {
#ifdef LUCENE_LOCK_DIR
    LOCK_DIR = LUCENE_LOCK_DIR;
#else
#ifdef LUCENE_LOCK_DIR_ENV_1
    if (LOCK_DIR == NULL) LOCK_DIR = getenv(LUCENE_LOCK_DIR_ENV_1);
#endif
#ifdef LUCENE_LOCK_DIR_ENV_2
    if (LOCK_DIR == NULL) LOCK_DIR = getenv(LUCENE_LOCK_DIR_ENV_2);
#endif
#ifdef LUCENE_LOCK_DIR_ENV_FALLBACK
    if (LOCK_DIR == NULL) LOCK_DIR = LUCENE_LOCK_DIR_ENV_FALLBACK;
#endif
    if (LOCK_DIR == NULL) _CLTHROWA(CL_ERR_IO, "Couldn't get determine lock dir");
#endif

    return LOCK_DIR;
}

DorisCompoundDirectory::DorisCompoundDirectory() : Directory(), useMMap(LUCENE_USE_MMAP) {
    filemode = 0644;
    this->lockFactory = NULL;
}

void DorisCompoundDirectory::init(const io::FileSystemSPtr& _fs, const char* _path,
                                  lucene::store::LockFactory* lockFactory, const io::FileSystemSPtr& cfs,
                                  const char* cfs_path) {
    fs = _fs;
    directory = _path;

    if (cfs == nullptr) {
        compound_fs = fs;
    } else {
        compound_fs = cfs;
    }
    if (cfs_path != nullptr) {
        cfs_directory = cfs_path;
    } else {
        cfs_directory = _path;
    }
    bool doClearLockID = false;

    if (lockFactory == NULL) {
        if (disableLocks) {
            lockFactory = lucene::store::NoLockFactory::getNoLockFactory();
        } else {
            lockFactory = _CLNEW lucene::store::FSLockFactory(directory.c_str(), this->filemode);
            doClearLockID = true;
        }
    }

    setLockFactory(lockFactory);

    if (doClearLockID) {
        lockFactory->setLockPrefix(NULL);
    }

    // It's meaningless checking directory existence in S3.
    if (fs->type() == io::FileSystemType::S3) {
        return;
    }
    bool exists = false;
    Status status = fs->exists(directory, &exists);
    if (!status.ok()) {
        auto err = "File system error: " + status.get_error_msg();
        LOG(WARNING) << err;
        _CLTHROWA_DEL(CL_ERR_IO, err.c_str());
    }
    if (!exists) {
        auto e = "Doris compound directory init error: " + directory + " is not a directory";
        LOG(WARNING) << e;
        _CLTHROWA_DEL(CL_ERR_IO, e.c_str());
    }
}

void DorisCompoundDirectory::create() {
    // SCOPED_LOCK_MUTEX(THIS_LOCK)
    std::lock_guard<doris::Mutex> wlock(_this_lock);

    //clear old files
    std::vector<std::string> files;
    lucene::util::Misc::listFiles(directory.c_str(), files, false); // TODO fs.list
    std::vector<std::string>::iterator itr = files.begin();
    while (itr != files.end()) {
        if (CL_NS(index)::IndexReader::isLuceneFile(itr->c_str())) {
            if (unlink((directory + PATH_DELIMITERA + *itr).c_str()) == -1) { // TODO
                _CLTHROWA(CL_ERR_IO, "Couldn't delete file "); //todo: make richer error
            }
        }
        itr++;
    }
    lockFactory->clearLock(CL_NS(index)::IndexWriter::WRITE_LOCK_NAME);
}

void DorisCompoundDirectory::priv_getFN(char* buffer, const char* name) const {
    buffer[0] = 0;
    strcpy(buffer, directory.c_str());
    strcat(buffer, PATH_DELIMITERA);
    strcat(buffer, name);
}

DorisCompoundDirectory::~DorisCompoundDirectory() {}

void DorisCompoundDirectory::setFileMode(int mode) {
    this->filemode = mode;
}
int DorisCompoundDirectory::getFileMode() {
    return this->filemode;
}
void DorisCompoundDirectory::setUseMMap(bool value) {
    useMMap = value;
}
bool DorisCompoundDirectory::getUseMMap() const {
    return useMMap;
}
const char* DorisCompoundDirectory::getClassName() {
    return "DorisCompoundDirectory";
}
const char* DorisCompoundDirectory::getObjectName() const {
    return getClassName();
}

void DorisCompoundDirectory::setDisableLocks(bool doDisableLocks) {
    disableLocks = doDisableLocks;
}
bool DorisCompoundDirectory::getDisableLocks() {
    return disableLocks;
}

bool DorisCompoundDirectory::list(std::vector<std::string>* names) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    std::vector<std::filesystem::path> paths;
    RETURN_IF_ERROR(fs->list(fl, &paths));
    for (auto path : paths) {
        names->push_back(path.string());
    }
    return true;
}

bool DorisCompoundDirectory::fileExists(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    bool exists = false;
    fs->exists(fl, &exists);
    return exists;
}

const char* DorisCompoundDirectory::getDirName() const {
    return directory.c_str();
}

const char* DorisCompoundDirectory::getCfsDirName() const {
    return cfs_directory.c_str();
}

DorisCompoundDirectory* DorisCompoundDirectory::getDirectory(
        const io::FileSystemSPtr& fileSystem, const char* file, bool create,
        lucene::store::LockFactory* lockFactory) {
    DorisCompoundDirectory* dir = getDirectory(fileSystem, file, (lucene::store::LockFactory*)NULL);

    // This is now deprecated (creation should only be done
    // by IndexWriter):
    if (create) {
        dir->create();
    }

    return dir;
}

DorisCompoundDirectory* DorisCompoundDirectory::getDirectory(const io::FileSystemSPtr& fs, const char* file,
                                                             bool useCompoundFileWriter,
                                                             const io::FileSystemSPtr& cfs_fs,
                                                             const char* cfs_file) {
    DorisCompoundDirectory* dir =
            getDirectory(fs, file, (lucene::store::LockFactory*)nullptr, cfs_fs, cfs_file);
    dir->useCompoundFileWriter = useCompoundFileWriter;
    return dir;
}

//static
DorisCompoundDirectory* DorisCompoundDirectory::getDirectory(
        const io::FileSystemSPtr& fs, const char* _file, lucene::store::LockFactory* lockFactory,
        const io::FileSystemSPtr& cfs, const char* _cfs_file) {
    const char* cfs_file = _cfs_file;
    if (cfs_file == nullptr) {
        cfs_file = _file;
    }
    DorisCompoundDirectory* dir = nullptr;
    if (!_file || !*_file) {
        _CLTHROWA(CL_ERR_IO, "Invalid directory");
    }

    const char* file = _file; // TODO xk

    bool exists = false;
    fs->exists(file, &exists);
    if (!exists) {
        mkdir(file, 0777); // TODO xk
    }

    dir = _CLNEW DorisCompoundDirectory();
    dir->init(fs, file, lockFactory, cfs, cfs_file);

    return dir; // TODO: Isn't this a double ref increment?
}

int64_t DorisCompoundDirectory::fileModified(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    struct stat64 buf;
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    if (stat64(buffer, &buf) == -1) // TODO xk
        return 0;
    else
        return buf.st_mtime;
}

//static
int64_t DorisCompoundDirectory::fileModified(const char* dir, const char* name) {
    struct stat64 buf;
    char buffer[CL_MAX_DIR];
    snprintf(buffer, CL_MAX_DIR, "%s%s%s", dir, PATH_DELIMITERA, name);
    stat64(buffer, &buf); // TODO xk
    return buf.st_mtime;
}

void DorisCompoundDirectory::touchFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    snprintf(buffer, CL_MAX_DIR, "%s%s%s", directory.c_str(), PATH_DELIMITERA, name);

    // int32_t r = _cl_open(buffer, O_RDWR, this->filemode);
    // if (r < 0) _CLTHROWA(CL_ERR_IO, "IO Error while touching file");
    // ::_close(r);

    io::FileWriterPtr tmp_writer;
    if (!fs->create_file(buffer, &tmp_writer).ok())
        _CLTHROWA(CL_ERR_IO, "IO Error while touching file");
}

int64_t DorisCompoundDirectory::fileLength(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    size_t size = 0;
    RETURN_IF_ERROR(fs->file_size(buffer, &size));
    return size;
}

bool DorisCompoundDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                       CLuceneError& error, int32_t bufferSize) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
#ifdef LUCENE_FS_MMAP
    //todo: do some tests here... like if the file
    //is >2gb, then some system cannot mmap the file
    //also some file systems mmap will fail?? could detect here too
    if (useMMap &&
        Misc::file_Size(fl) <
                LUCENE_INT32_MAX_SHOULDBE) //todo: would this be bigger on 64bit systems?. i suppose it would be...test first
        return MMapIndexInput::open(fl, ret, error, bufferSize);
    else
#endif
        return FSIndexInput::open(fs, fl, ret, error, bufferSize);
}

void DorisCompoundDirectory::close() {
    if (useCompoundFileWriter) {
        DorisCompoundFileWriter* cfsWriter = _CLNEW DorisCompoundFileWriter(this);
        // write compound file
        cfsWriter->writeCompoundFile();
        // delete index path, which contains separated inverted index files
        deleteDirectory();
        _CLDELETE(cfsWriter);
    }
}

/**
   * So we can do some byte-to-hexchar conversion below
   */
char HEX_DIGITS[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                     '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

char* DorisCompoundDirectory::getLockPrefix() const {
    char dirName[CL_MAX_PATH]; // name to be hashed
    if (realpath(directory.c_str(), dirName) == NULL) {
        _CLTHROWA(CL_ERR_Runtime, "Invalid directory path");
    }

    //to make a compatible name with jlucene, we need to make some changes...
    if (dirName[1] == ':') dirName[0] = (char)toupper((char)dirName[0]);

    // char* smd5 = lucene::util::MD5String(dirName);

    Md5Digest digest;
    digest.update(dirName, CL_MAX_PATH);
    digest.digest();
    const char* smd5 = digest.hex().c_str();

    char* ret = _CL_NEWARRAY(char, 32 + 7 + 1); //32=2*16, 7=strlen("lucene-")
    strcpy(ret, "lucene-");
    strcat(ret, smd5);

    // _CLDELETE_CaARRAY(smd5);
    delete[] smd5;

    return ret;
}

bool DorisCompoundDirectory::doDeleteFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    // return _unlink(fl) != -1;
    return fs->delete_file(fl).ok();
}

bool DorisCompoundDirectory::deleteDirectory() {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    Status status = fs->delete_directory(fl);
    if (!status.ok()) {
        char* err = _CL_NEWARRAY(
                char, 16 + status.get_error_msg().length() + 1); //16: len of "couldn't delete "
        strcpy(err, "couldn't delete directory: ");
        strcat(err, status.get_error_msg().c_str());
        _CLTHROWA_DEL(CL_ERR_IO, err);
    }
    return true;
}

void DorisCompoundDirectory::renameFile(const char* from, const char* to) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    // SCOPED_LOCK_MUTEX(THIS_LOCK)
    std::lock_guard<doris::Mutex> wlock(_this_lock);
    char old[CL_MAX_DIR];
    priv_getFN(old, from);

    char nu[CL_MAX_DIR];
    priv_getFN(nu, to);

    /* This is not atomic.  If the program crashes between the call to
    delete() and the call to renameTo() then we're screwed, but I've
    been unable to figure out how else to do this... */

    bool exists = false;
    fs->exists(nu, &exists);
    // if (lucene::util::Misc::dir_Exists(nu)) {
    if (exists) {
        //we run this sequence of unlinking an arbitary 100 times
        //on some platforms (namely windows), there can be a
        //delay between unlink and dir_exists==false
        // if (Misc::file_Unlink(nu) == -1) {
        if (!fs->delete_directory(nu).ok()) {
            char* err = _CL_NEWARRAY(char, 16 + strlen(to) + 1); //16: len of "couldn't delete "
            strcpy(err, "couldn't delete ");
            strcat(err, to);
            _CLTHROWA_DEL(CL_ERR_IO, err);
        }
    }
    if (rename(old, nu) != 0) { // TODO xk
        //todo: jlucene has some extra rename code - if the rename fails, it copies
        //the whole file to the new file... might want to implement that if renaming
        //fails on some platforms
        char buffer[20 + CL_MAX_PATH + CL_MAX_PATH];
        strcpy(buffer, "couldn't rename ");
        strcat(buffer, from);
        strcat(buffer, " to ");
        strcat(buffer, nu);
        _CLTHROWA(CL_ERR_IO, buffer);
    }
}

lucene::store::IndexOutput* DorisCompoundDirectory::createOutput(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    bool exists = false;
    auto status = fs->exists(fl, &exists);
    if (!status.ok()) {
        LOG(WARNING) << "Doris compound directory create output error: " << status.get_error_msg();
        return nullptr;
    }
    if (exists) {
        if (!fs->delete_file(fl).ok()) {
            char tmp[1024];
            strcpy(tmp, "Cannot overwrite: ");
            strcat(tmp, name);
            _CLTHROWA(CL_ERR_IO, tmp);
        }
        fs->exists(fl, &exists);
        assert(!exists);
    }
    auto ret = _CLNEW FSIndexOutput();
    try {
        ret->init(fs, fl);
    } catch(CLuceneError& err) {
        LOG(WARNING) << "FSIndexOutput init error: " << err.what();
        _CLTHROWA(CL_ERR_IO, "FSIndexOutput init error");
    }
    return ret;
}

std::string DorisCompoundDirectory::toString() const {
    return std::string("DorisCompoundDirectory@") + this->directory;
}

} // namespace segment_v2
} // namespace doris
