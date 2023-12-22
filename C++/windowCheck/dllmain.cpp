// dllmain.cpp : Defines the entry point for the DLL application.
#include "pch.h"
#include "file_engine_dllInterface_WindowCheck.h"
#include <Windows.h>
#pragma comment(lib, "User32")

BOOL isForegroundFullscreen();

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

/*
 * Class:     file_engine_dllInterface_WindowCheck
 * Method:    isForegroundFullscreen
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_file_engine_dllInterface_WindowCheck_isForegroundFullscreen
(JNIEnv*, jobject)
{
    return static_cast<jboolean>(isForegroundFullscreen());
}

BOOL isForegroundFullscreen()
{
    bool b_fullscreen = false;
    RECT rc_app;
    RECT rc_desk;

    const HWND hWnd = GetForegroundWindow();

    if (hWnd != GetDesktopWindow() && hWnd != GetShellWindow())
    {
        GetWindowRect(hWnd, &rc_app);
        GetWindowRect(GetDesktopWindow(), &rc_desk);

        if (rc_app.left <= rc_desk.left &&
            rc_app.top <= rc_desk.top &&
            rc_app.right >= rc_desk.right &&
            rc_app.bottom >= rc_desk.bottom)
        {
            char szTemp[100]{0};

            if (GetClassNameA(hWnd, szTemp, sizeof(szTemp)) > 0)
            {
                if (strcmp(szTemp, "WorkerW") != 0)
                    b_fullscreen = true;
            }
            else
            {
                b_fullscreen = true;
            }
        }
    }
    return b_fullscreen;
}
