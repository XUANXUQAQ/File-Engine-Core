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
        DWORD ppid = 0;
        std::set<DWORD> process_set;
        do
        {
            process_set.insert(pe.th32ProcessID);
            if (pe.th32ProcessID == pid)
            {
                ppid = pe.th32ParentProcessID;
            }
            if (ppid != 0)
            {
                const auto&& ppid_iter = process_set.find(ppid);
                if (ppid_iter != process_set.end())
                {
                    CloseHandle(h);
                    return true;
                }
            }
        }
        while (Process32Next(h, &pe));
    }
    CloseHandle(h);
    return false;
}
