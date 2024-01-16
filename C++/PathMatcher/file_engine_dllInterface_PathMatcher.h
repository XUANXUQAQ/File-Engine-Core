/* DO NOT EDIT THIS FILE - it is machine generated */
#include "jni.h"
/* Header for class file_engine_dllInterface_PathMatcher */

#ifndef _Included_file_engine_dllInterface_PathMatcher
#define _Included_file_engine_dllInterface_PathMatcher
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     file_engine_dllInterface_PathMatcher
 * Method:    match
 * Signature: (Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;ZLjava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[ZI)[Ljava/lang/String;
 */
JNIEXPORT jobjectArray JNICALL Java_file_engine_dllInterface_PathMatcher_match
  (JNIEnv *, jobject, jstring, jstring, jobjectArray, jboolean, jstring, jobjectArray, jobjectArray, jbooleanArray, jint);

/*
 * Class:     file_engine_dllInterface_PathMatcher
 * Method:    openConnection
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_file_engine_dllInterface_PathMatcher_openConnection
  (JNIEnv *, jobject, jstring);

/*
 * Class:     file_engine_dllInterface_PathMatcher
 * Method:    closeConnections
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_file_engine_dllInterface_PathMatcher_closeConnections
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif