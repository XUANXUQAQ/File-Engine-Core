#include "constants.h"
#include "search.h"
#ifdef TEST
#include <iostream>
#endif
#include <unordered_map>
#include <string>
#include <vector>
#include <algorithm>
#include "sqlite3.h"
#include <concurrent_unordered_set.h>
#include <mutex>
#include "ntfs.h"


volume::volume(const char vol, sqlite3* database, std::vector<std::string>* ignore_paths, PriorityMap* priority_map)
{
    this->vol = vol;
    this->priority_map_ = priority_map;
    hVol = nullptr;
    db = database;
    ignore_path_vector_ = ignore_paths;
}

volume::~volume()
{
    CloseHandle(hVol);
}

void volume::init_volume()
{
    if (get_handle() && get_ntfs_volume_data())
    {
        using namespace std;
        auto collect_internal = [this](const Frn_Pfrn_Name_Map::iterator& map_iterator)
        {
            unsigned count = 0;
            const auto& name = map_iterator->second.filename;
            const int ascii = get_asc_ii_sum(name);
            std::string result_path;
            get_path(map_iterator->first, result_path);
            if (const auto record = vol + result_path; !is_ignore(record))
            {
                collect_result_to_result_map(ascii, record);
                ++count;
            }
            return count;
        };
        auto search_internal = [this, &collect_internal]
        {
            auto&& start_iter = frnPfrnNameMap.begin();
            auto&& end_iter = frnPfrnNameMap.end();
            unsigned count = 0;
            init_all_prepare_statement();
            while (start_iter != end_iter)
            {
                count += collect_internal(start_iter);
                ++start_iter;
                if (count > SAVE_TO_DATABASE_RECORD_CHECKPOINT)
                {
                    count = 0;
                    finalize_all_statement();
                    init_all_prepare_statement();
                }
            }
            finalize_all_statement();
        };
        try
        {
            search_internal();
            printf("collect disk %c complete.\n", this->getDiskPath());
        }
        catch (exception& e)
        {
            fprintf(stderr, "fileSearcherUSN: %s\n", e.what());
        }
    }
    else
    {
        fprintf(stderr, "fileSearcherUSN: init usn journal failed.\n");
    }
    auto&& info = std::string("disk ") + this->getDiskPath() + " complete";
    printf("%s\n", info.c_str());
}

void volume::collect_result_to_result_map(const int ascii, const std::string& full_path) const
{
    int ascii_group = ascii / 100;
    if (ascii_group > 40)
    {
        ascii_group = 40;
    }
    const int priority = get_priority_by_path(full_path);

    save_result(full_path, ascii, ascii_group, priority);
}

int volume::get_priority_by_suffix(const std::string& suffix) const
{
    auto&& iter = priority_map_->find(suffix);
    if (iter == priority_map_->end())
    {
        if (suffix.find('\\') != std::string::npos)
        {
            return get_priority_by_suffix("dirPriority");
        }
        return get_priority_by_suffix("defaultPriority");
    }
    return iter->second;
}


int volume::get_priority_by_path(const std::string& _path) const
{
    auto&& suffix = _path.substr(_path.find_last_of('.') + 1);
    transform(suffix.begin(), suffix.end(), suffix.begin(), tolower);
    return get_priority_by_suffix(suffix);
}

void volume::init_single_prepare_statement(sqlite3_stmt** statement, const char* init) const
{
    if (const size_t ret = sqlite3_prepare_v2(db, init, static_cast<long>(strlen(init)), statement, nullptr); SQLITE_OK
        != ret)
    {
        auto&& err_info = std::string("error preparing stmt \"") + init + "\"  disk: " + this->getDiskPath();
        fprintf(stderr, "fileSearcherUSN: %s\n", err_info.c_str());
    }
}

void volume::finalize_all_statement() const
{
    sqlite3_finalize(stmt0);
    sqlite3_finalize(stmt1);
    sqlite3_finalize(stmt2);
    sqlite3_finalize(stmt3);
    sqlite3_finalize(stmt4);
    sqlite3_finalize(stmt5);
    sqlite3_finalize(stmt6);
    sqlite3_finalize(stmt7);
    sqlite3_finalize(stmt8);
    sqlite3_finalize(stmt9);
    sqlite3_finalize(stmt10);
    sqlite3_finalize(stmt11);
    sqlite3_finalize(stmt12);
    sqlite3_finalize(stmt13);
    sqlite3_finalize(stmt14);
    sqlite3_finalize(stmt15);
    sqlite3_finalize(stmt16);
    sqlite3_finalize(stmt17);
    sqlite3_finalize(stmt18);
    sqlite3_finalize(stmt19);
    sqlite3_finalize(stmt20);
    sqlite3_finalize(stmt21);
    sqlite3_finalize(stmt22);
    sqlite3_finalize(stmt23);
    sqlite3_finalize(stmt24);
    sqlite3_finalize(stmt25);
    sqlite3_finalize(stmt26);
    sqlite3_finalize(stmt27);
    sqlite3_finalize(stmt28);
    sqlite3_finalize(stmt29);
    sqlite3_finalize(stmt30);
    sqlite3_finalize(stmt31);
    sqlite3_finalize(stmt32);
    sqlite3_finalize(stmt33);
    sqlite3_finalize(stmt34);
    sqlite3_finalize(stmt35);
    sqlite3_finalize(stmt36);
    sqlite3_finalize(stmt37);
    sqlite3_finalize(stmt38);
    sqlite3_finalize(stmt39);
    sqlite3_finalize(stmt40);
    sqlite3_exec(db, "commit;", nullptr, nullptr, nullptr);
}

void volume::save_single_record_to_db(sqlite3_stmt* stmt, const std::string& record, const int ascii,
                                      const int priority)
{
    sqlite3_reset(stmt);
    sqlite3_bind_int(stmt, 1, ascii);
    sqlite3_bind_text(stmt, 2, record.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 3, priority);
    sqlite3_step(stmt);
}

void volume::init_all_prepare_statement()
{
    sqlite3_exec(db, "begin;", nullptr, nullptr, nullptr);
    init_single_prepare_statement(&stmt0, "INSERT OR IGNORE INTO list0 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt1, "INSERT OR IGNORE INTO list1 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt2, "INSERT OR IGNORE INTO list2 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt3, "INSERT OR IGNORE INTO list3 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt4, "INSERT OR IGNORE INTO list4 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt5, "INSERT OR IGNORE INTO list5 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt6, "INSERT OR IGNORE INTO list6 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt7, "INSERT OR IGNORE INTO list7 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt8, "INSERT OR IGNORE INTO list8 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt9, "INSERT OR IGNORE INTO list9 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt10, "INSERT OR IGNORE INTO list10 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt11, "INSERT OR IGNORE INTO list11 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt12, "INSERT OR IGNORE INTO list12 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt13, "INSERT OR IGNORE INTO list13 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt14, "INSERT OR IGNORE INTO list14 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt15, "INSERT OR IGNORE INTO list15 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt16, "INSERT OR IGNORE INTO list16 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt17, "INSERT OR IGNORE INTO list17 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt18, "INSERT OR IGNORE INTO list18 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt19, "INSERT OR IGNORE INTO list19 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt20, "INSERT OR IGNORE INTO list20 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt21, "INSERT OR IGNORE INTO list21 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt22, "INSERT OR IGNORE INTO list22 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt23, "INSERT OR IGNORE INTO list23 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt24, "INSERT OR IGNORE INTO list24 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt25, "INSERT OR IGNORE INTO list25 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt26, "INSERT OR IGNORE INTO list26 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt27, "INSERT OR IGNORE INTO list27 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt28, "INSERT OR IGNORE INTO list28 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt29, "INSERT OR IGNORE INTO list29 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt30, "INSERT OR IGNORE INTO list30 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt31, "INSERT OR IGNORE INTO list31 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt32, "INSERT OR IGNORE INTO list32 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt33, "INSERT OR IGNORE INTO list33 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt34, "INSERT OR IGNORE INTO list34 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt35, "INSERT OR IGNORE INTO list35 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt36, "INSERT OR IGNORE INTO list36 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt37, "INSERT OR IGNORE INTO list37 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt38, "INSERT OR IGNORE INTO list38 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt39, "INSERT OR IGNORE INTO list39 VALUES(?, ?, ?);");
    init_single_prepare_statement(&stmt40, "INSERT OR IGNORE INTO list40 VALUES(?, ?, ?);");
}

bool volume::is_ignore(const std::string& _path) const
{
    if (_path.find('$') != std::string::npos)
    {
        return true;
    }
    std::string path0(_path);
    transform(path0.begin(), path0.end(), path0.begin(), tolower);
    return std::any_of(ignore_path_vector_->begin(), ignore_path_vector_->end(), [path0](const std::string& each)
    {
        return path0.find(each) != std::string::npos;
    });
}

void volume::save_result(const std::string& _path, const int ascii, const int ascii_group, const int priority) const
{
    switch (ascii_group)
    {
    case 0:
        save_single_record_to_db(stmt0, _path, ascii, priority);
        break;
    case 1:
        save_single_record_to_db(stmt1, _path, ascii, priority);
        break;
    case 2:
        save_single_record_to_db(stmt2, _path, ascii, priority);
        break;
    case 3:
        save_single_record_to_db(stmt3, _path, ascii, priority);
        break;
    case 4:
        save_single_record_to_db(stmt4, _path, ascii, priority);
        break;
    case 5:
        save_single_record_to_db(stmt5, _path, ascii, priority);
        break;
    case 6:
        save_single_record_to_db(stmt6, _path, ascii, priority);
        break;
    case 7:
        save_single_record_to_db(stmt7, _path, ascii, priority);
        break;
    case 8:
        save_single_record_to_db(stmt8, _path, ascii, priority);
        break;
    case 9:
        save_single_record_to_db(stmt9, _path, ascii, priority);
        break;
    case 10:
        save_single_record_to_db(stmt10, _path, ascii, priority);
        break;
    case 11:
        save_single_record_to_db(stmt11, _path, ascii, priority);
        break;
    case 12:
        save_single_record_to_db(stmt12, _path, ascii, priority);
        break;
    case 13:
        save_single_record_to_db(stmt13, _path, ascii, priority);
        break;
    case 14:
        save_single_record_to_db(stmt14, _path, ascii, priority);
        break;
    case 15:
        save_single_record_to_db(stmt15, _path, ascii, priority);
        break;
    case 16:
        save_single_record_to_db(stmt16, _path, ascii, priority);
        break;
    case 17:
        save_single_record_to_db(stmt17, _path, ascii, priority);
        break;
    case 18:
        save_single_record_to_db(stmt18, _path, ascii, priority);
        break;
    case 19:
        save_single_record_to_db(stmt19, _path, ascii, priority);
        break;
    case 20:
        save_single_record_to_db(stmt20, _path, ascii, priority);
        break;
    case 21:
        save_single_record_to_db(stmt21, _path, ascii, priority);
        break;
    case 22:
        save_single_record_to_db(stmt22, _path, ascii, priority);
        break;
    case 23:
        save_single_record_to_db(stmt23, _path, ascii, priority);
        break;
    case 24:
        save_single_record_to_db(stmt24, _path, ascii, priority);
        break;
    case 25:
        save_single_record_to_db(stmt25, _path, ascii, priority);
        break;
    case 26:
        save_single_record_to_db(stmt26, _path, ascii, priority);
        break;
    case 27:
        save_single_record_to_db(stmt27, _path, ascii, priority);
        break;
    case 28:
        save_single_record_to_db(stmt28, _path, ascii, priority);
        break;
    case 29:
        save_single_record_to_db(stmt29, _path, ascii, priority);
        break;
    case 30:
        save_single_record_to_db(stmt30, _path, ascii, priority);
        break;
    case 31:
        save_single_record_to_db(stmt31, _path, ascii, priority);
        break;
    case 32:
        save_single_record_to_db(stmt32, _path, ascii, priority);
        break;
    case 33:
        save_single_record_to_db(stmt33, _path, ascii, priority);
        break;
    case 34:
        save_single_record_to_db(stmt34, _path, ascii, priority);
        break;
    case 35:
        save_single_record_to_db(stmt35, _path, ascii, priority);
        break;
    case 36:
        save_single_record_to_db(stmt36, _path, ascii, priority);
        break;
    case 37:
        save_single_record_to_db(stmt37, _path, ascii, priority);
        break;
    case 38:
        save_single_record_to_db(stmt38, _path, ascii, priority);
        break;
    case 39:
        save_single_record_to_db(stmt39, _path, ascii, priority);
        break;
    case 40:
        save_single_record_to_db(stmt40, _path, ascii, priority);
        break;
    default:
        break;
    }
}

int volume::get_asc_ii_sum(const std::string& name)
{
    auto sum = 0;
    const auto length = name.length();
    for (size_t i = 0; i < length; i++)
    {
        if (name[i] > 0)
        {
            sum += name[i];
        }
    }
    return sum;
}

void volume::get_path(DWORDLONG frn, std::string& output_path)
{
    const auto end = frnPfrnNameMap.end();
    while (true)
    {
        auto it = frnPfrnNameMap.find(frn);
        if (it == end)
        {
            output_path = ":" + output_path;
            return;
        }
        output_path = "\\" + it->second.filename + output_path;
        frn = it->second.pfrn;
    }
}

bool volume::get_handle()
{
    // 为\\?\C:的形式
    CString lp_file_name(_T("\\\\?\\c:"));
    lp_file_name.SetAt(4, vol);


    hVol = CreateFile(lp_file_name,
                      GENERIC_READ | GENERIC_WRITE, // 可以为0
                      FILE_SHARE_READ | FILE_SHARE_WRITE, // 必须包含有FILE_SHARE_WRITE
                      nullptr,
                      OPEN_EXISTING, // 必须包含OPEN_EXISTING, CREATE_ALWAYS可能会导致错误
                      0, // FILE_ATTRIBUTE_NORMAL可能会导致错误
                      nullptr);

    if (INVALID_HANDLE_VALUE != hVol)
    {
        return true;
    }
    auto&& info = std::wstring(L"create file handle failed. ") + lp_file_name.GetString() +
        L"error code: " + std::to_wstring(GetLastError());
    fprintf(stderr, "fileSearcherUSN: %ls", info.c_str());
    return false;
}

bool volume::get_ntfs_volume_data()
{
    DWORD bytes_read = 0;
    NTFS_VOLUME_DATA_BUFFER volume_data;
    if (!DeviceIoControl(hVol,
                         FSCTL_GET_NTFS_VOLUME_DATA,
                         nullptr,
                         0,
                         &volume_data,
                         sizeof(volume_data),
                         &bytes_read,
                         nullptr))
    {
        fprintf(stderr, "fileSearcherUSN: %s\n", "Failed to read NTFS volume data.");
        CloseHandle(hVol);
        return false;
    }

    auto mft_count = volume_data.MftValidDataLength.QuadPart / volume_data.BytesPerFileRecordSegment;

    NTFS_FILE_RECORD_INPUT_BUFFER input_buffer;
    auto output_buffer_size = sizeof(NTFS_FILE_RECORD_OUTPUT_BUFFER) + volume_data.BytesPerFileRecordSegment - 1;
    PNTFS_FILE_RECORD_OUTPUT_BUFFER output_buffer = static_cast<PNTFS_FILE_RECORD_OUTPUT_BUFFER>(
        malloc(output_buffer_size));
    DeviceIoControl(hVol, FSCTL_GET_NTFS_FILE_RECORD, &input_buffer, sizeof(input_buffer), output_buffer,
                    static_cast<DWORD>(output_buffer_size), &bytes_read, nullptr);

    for (DWORD index = static_cast<DWORD>(mft_count) - 1; index >= 16; --index)
    {
        input_buffer.FileReferenceNumber.LowPart = index;
        if (!DeviceIoControl(hVol, FSCTL_GET_NTFS_FILE_RECORD, &input_buffer, sizeof(input_buffer), output_buffer,
                             static_cast<DWORD>(output_buffer_size), &bytes_read, nullptr))
        {
            fprintf(stderr, "fileSearcherUSN: %s\n", "Failed to read MFT record.");
            return false;
        }

        // 跳过空记录
        index = output_buffer->FileReferenceNumber.LowPart;

        PFILE_RECORD_SEGMENT_HEADER p_header = reinterpret_cast<PFILE_RECORD_SEGMENT_HEADER>(output_buffer->
            FileRecordBuffer);
        if (p_header->Flags & 0x0004) // msdn 未定义，跳过
            continue;
        if (!(p_header->Flags & 0x0001)) // 非标准文件
            continue;
        if (p_header->SequenceNumber == 0) // 过时条目
            continue;

        PATTRIBUTE_RECORD_HEADER p_attr = reinterpret_cast<PATTRIBUTE_RECORD_HEADER>(reinterpret_cast<size_t>(p_header)
            + p_header->FirstAttributeOffset);
        while (p_attr->TypeCode != 0xFFFFFFFF)
        {
            auto typeCode = p_attr->TypeCode;
            auto resident = p_attr->FormCode;

            if (resident == 0x00)
            {
                auto form = p_attr->Form.Resident;
                void* ptr = reinterpret_cast<void*>(reinterpret_cast<size_t>(p_attr) + form.ValueOffset);

                if (typeCode == 0x30) //$FILE_NAME
                {
                    if (PFILE_NAME p_file_name = static_cast<PFILE_NAME>(ptr); p_file_name->Flags & 0x01) // NTFS 长文件名
                    {
                        auto filename = p_file_name->FileName;
                        auto filename_length = p_file_name->FileNameLength;

                        auto parent_id = p_file_name->ParentDirectory.SegmentNumberLowPart;
                        auto id = index;
                        PFRN_NAME pfrn_name;
                        pfrn_name.filename = to_utf8(std::wstring(filename, filename_length));
                        pfrn_name.pfrn = parent_id;
                        frnPfrnNameMap.insert(std::make_pair(id, pfrn_name));
                    }
                }
            }

            // next
            p_attr = reinterpret_cast<PATTRIBUTE_RECORD_HEADER>(reinterpret_cast<size_t>(p_attr) + p_attr->
                RecordLength);
        }
    }
    free(output_buffer);
    return true;
}

std::string get_file_name(const std::string& path)
{
    std::string file_name = path.substr(path.find_last_of('\\') + 1);
    return file_name;
}
