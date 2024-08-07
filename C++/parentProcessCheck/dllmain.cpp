// dllmain.cpp : Defines the entry point for the DLL application.
#include "pch.h"

#include <Windows.h>
#include "file_engine_dllInterface_ParentProcessCheck.h"
#include <tlhelp32.h>
#include <set>

DWORD get_parent_process_id();

BOOL APIENTRY DllMain(HMODULE hModule,
                      DWORD ul_reason_for_call,
                      LPVOID lpReserved
)
{
    switch (ul_reason_for_call)
    {
    case DLL_PROCESS_ATTACH:
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
    case DLL_PROCESS_DETACH:
        break;
    }
    return TRUE;
}

JNIEXPORT jboolean JNICALL Java_file_engine_dllInterface_ParentProcessCheck_isParentProcessExist
(JNIEnv*, jobject)
{
    const auto&& ppid = get_parent_process_id();
    if (ppid == 0)
    {
        return false;
    }
    const auto&& p_handle = OpenProcess(PROCESS_QUERY_INFORMATION, false, ppid);
    if (p_handle == nullptr)
    {
        return false;
    }
    DWORD exit_code{};
    // check for status
    const bool still_exist = GetExitCodeProcess(p_handle, &exit_code) &&
        exit_code == STILL_ACTIVE;
    CloseHandle(p_handle);
    return still_exist;
}

DWORD get_parent_process_id()
{
    const HANDLE h = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
    PROCESSENTRY32 pe{};
    pe.dwSize = sizeof(PROCESSENTRY32);

    const auto pid = GetCurrentProcessId();
    DWORD ppid = 0;

    if (Process32First(h, &pe))
    {
        do
        {
            if (pe.th32ProcessID == pid)
            {
                ppid = pe.th32ParentProcessID;
                break;
            }
        }
        while (Process32Next(h, &pe));
    }
    CloseHandle(h);
    return ppid;
}