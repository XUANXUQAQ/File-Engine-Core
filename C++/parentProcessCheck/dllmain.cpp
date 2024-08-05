// dllmain.cpp : Defines the entry point for the DLL application.
#include "pch.h"

#include <Windows.h>
#include "file_engine_dllInterface_ParentProcessCheck.h"
#include <tlhelp32.h>
#include <set>

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
    const HANDLE h = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
    PROCESSENTRY32 pe{};
    pe.dwSize = sizeof(PROCESSENTRY32);

    const auto pid = GetCurrentProcessId();

    if (Process32First(h, &pe))
    {
        do
        {
            if (pe.th32ProcessID == pid)
            {
                const auto ppid = pe.th32ParentProcessID;
                // get process handle
                const auto&& p_handle = OpenProcess(PROCESS_QUERY_INFORMATION, false, ppid);
                if (p_handle == nullptr)
                {
                    return false;
                }
                DWORD exit_code{};
                // check for status
                const bool still_exist = GetExitCodeProcess(p_handle, &exit_code) &&
                    exit_code == STILL_ACTIVE;
                return still_exist;
            }
        }
        while (Process32Next(h, &pe));
    }
    CloseHandle(h);
    return false;
}
