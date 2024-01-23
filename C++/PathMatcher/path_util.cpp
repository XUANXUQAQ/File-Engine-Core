#include "pch.h"
#include "path_util.h"
#include <regex>

#include "str_convert.h"

bool not_matched(const char* path,
                 const bool is_ignore_case,
                 const std::vector<std::string>* keywords,
                 const std::vector<std::string>* keywords_lower_case,
                 const int keywords_length,
                 const bool* is_keyword_path)
{
    for (int i = 0; i < keywords_length; ++i)
    {
        const bool is_keyword_path_val = is_keyword_path[i];
        char match_str[MAX_PATH_LENGTH]{0};
        if (is_keyword_path_val)
        {
            get_parent_path(path, match_str);
        }
        else
        {
            get_file_name(path, match_str);
        }
        std::string each_keyword;
        if (is_ignore_case)
        {
            each_keyword = keywords_lower_case->at(i);
            _strlwr_s(match_str);
        }
        else
        {
            each_keyword = keywords->at(i);
        }
        if (each_keyword.empty())
        {
            continue;
        }
        if (strstr(match_str, each_keyword.c_str()) == nullptr)
        {
            if (is_keyword_path_val || !is_str_contains_chinese(match_str))
            {
                return true;
            }
            char gbk_buffer[MAX_PATH_LENGTH * 2]{0};
            char* gbk_buffer_ptr = gbk_buffer;
            utf8_to_gbk(match_str, static_cast<unsigned>(strlen(match_str)), &gbk_buffer_ptr, nullptr);
            char converted_pinyin[MAX_PATH_LENGTH * 6]{0};
            char converted_pinyin_initials[MAX_PATH_LENGTH]{0};
            convert_to_pinyin(gbk_buffer, converted_pinyin, MAX_PATH_LENGTH * 6, converted_pinyin_initials);
            if (strstr(converted_pinyin, each_keyword.c_str()) == nullptr &&
                strstr(converted_pinyin_initials, each_keyword.c_str()) == nullptr)
            {
                return true;
            }
        }
    }
    return false;
}

void str_add_single(char* dst, const char c)
{
    while (*dst != '\0')
    {
        dst++;
    }
    *dst++ = c;
    *dst = '\0';
}

void convert_to_pinyin(const char* chinese_str, char* output_str, size_t output_size, char* pinyin_initials)
{
    const auto length = strlen(chinese_str);
    for (size_t j = 0; j < length;)
    {
        const unsigned char val = chinese_str[j];
        if (val < 128)
        {
            str_add_single(output_str, chinese_str[j]);
            str_add_single(pinyin_initials, chinese_str[j]);
            ++j;
            continue;
        }

        if (const int chrasc = chinese_str[j] * 256 + chinese_str[j + 1] + 256; chrasc > 0 && chrasc < 160)
        {
            str_add_single(output_str, chinese_str[j]);
            str_add_single(pinyin_initials, chinese_str[j]);
            ++j;
        }
        else
        {
            for (int i = sizeof spell_value / sizeof spell_value[0] - 1; i >= 0; --i)
            {
                if (spell_value[i] <= chrasc)
                {
                    strcat_s(output_str, output_size, spell_dict[i]);
                    str_add_single(pinyin_initials, spell_dict[i][0]);
                    break;
                }
            }
            j += 2;
        }
    }
}

bool is_str_contains_chinese(const char* source)
{
    int i = 0;
    while (source[i] != 0)
    {
        if (source[i] & 0x80 && source[i] & 0x40 && source[i] & 0x20)
        {
            return true;
        }
        if (source[i] & 0x80 && source[i] & 0x40)
        {
            i += 2;
        }
        else
        {
            i += 1;
        }
    }
    return false;
}

void get_file_name(const char* path, char* output)
{
    const char* p = strrchr(path, '\\');
    strcpy_s(output, MAX_PATH_LENGTH, p + 1);
}

void get_parent_path(const char* path, char* output)
{
    strcpy_s(output, MAX_PATH_LENGTH, path);
    char* p = strrchr(output, '\\');
    *p = '\0';
}

std::wstring string2wstring(const std::string& str)
{
    std::wstring result;
    const int len = MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), nullptr, 0);
    const auto buffer = new TCHAR[len + 1];
    MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), buffer, len);
    buffer[len] = '\0';
    result.append(buffer);
    delete[] buffer;
    return result;
}

inline bool is_file_exist(const std::string& path)
{
    struct _stat64i32 buffer;
    return _wstat(string2wstring(path).c_str(), &buffer) == 0;
}

int is_dir_or_file(const char* path)
{
    const auto w_path = string2wstring(path);
    const DWORD dwAttrib = GetFileAttributes(w_path.c_str());
    if (dwAttrib != INVALID_FILE_ATTRIBUTES)
    {
        if (dwAttrib & FILE_ATTRIBUTE_DIRECTORY)
        {
            return 0;
        }
        return 1;
    }
    return -1;
}

void tolowerstr(char* str);
bool match_func(const char* path, const search_info* info)
{
    if (path == nullptr || !path[0])
    {
        return false;
    }
    const auto search_case = info->get_search_case_num();
    if (search_case & 1 << 3)
    {
        // regex
        const std::regex re(info->get_search_text());
        if (auto&& search_res = std::regex_search(path, re); !search_res)
        {
            return false;
        }
    }
    else if (not_matched(path, info->is_ignore_search_case(), info->get_keywords(), info->get_keywords_lower_case(),
                         static_cast<int>(info->get_keywords()->size()),
                         info->is_keyword_path_value()))
    {
        return false;
    }
    if (search_case == 0)
    {
        return is_file_exist(path);
    }
    short is_all_case_matched = 0;
    if (search_case & 1)
    {
        // file
        if (is_dir_or_file(path) == 1)
        {
            is_all_case_matched |= 1;
        }
    }
    else
    {
        is_all_case_matched |= 1;
    }
    if (is_all_case_matched != 1)
    {
        return false;
    }
    if (search_case & 1 << 1)
    {
        // dir
        if (is_dir_or_file(path) == 0)
        {
            is_all_case_matched |= 2;
        }
    }
    else
    {
        is_all_case_matched |= 2;
    }
    if (is_all_case_matched != 3)
    {
        return false;
    }
    if (search_case & 1 << 2)
    {
        // full
        char search_text_arr[MAX_PATH_LENGTH]{0};
        strcpy_s(search_text_arr, info->get_search_text().c_str());
        char file_name[MAX_PATH_LENGTH]{0};
        get_file_name(path, file_name);

        tolowerstr(search_text_arr);
        tolowerstr(file_name);
        if (strcmp(search_text_arr, file_name) == 0)
        {
            is_all_case_matched |= 4;
        }
    }
    else
    {
        is_all_case_matched |= 4;
    }
    return is_all_case_matched == 7 && is_file_exist(path);
}

void tolowerstr(char* str)
{
    while (*str)
    {
        *str = static_cast<char>(std::tolower(*str));
        ++str;
    }
}