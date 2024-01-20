#include <memory>
#include <Windows.h>
#include <string>

std::wstring string2wstring(const std::string& str)
{
    const int buf_size = MultiByteToWideChar(CP_ACP,
                                             0,
                                             str.c_str(),
                                             -1,
                                             nullptr,
                                             0);
    const std::unique_ptr<wchar_t> wsp(new wchar_t[buf_size]);
    MultiByteToWideChar(CP_ACP,
                        0,
                        str.c_str(),
                        -1,
                        wsp.get(),
                        buf_size);
    std::wstring wstr(wsp.get());
    return wstr;
}
