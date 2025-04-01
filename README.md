<h1 align="center">File-Engine-Core</h1>

<div align="center">
  <strong>A tool can make you work more efficiently.</strong><br>
  <sub>File-Engine-Core，无UI，仅含有文件搜索以及文件监控功能，通过http api提供调用。</sub>
</div>
<br>
<div align="center">
  <img src="https://img.shields.io/badge/license-MIT-yellow"/>
  <img src="https://img.shields.io/badge/language-c++-brightgreen"/>
  <img src="https://img.shields.io/badge/language-java-brightgreen" />
  <img src="https://img.shields.io/badge/language-cuda-brightgreen"/>
  <img src="https://img.shields.io/badge/documentation-yes-brightgreen"/>
</div>

# 新项目推荐 Aiverything
使用File-Engine-Core，更易用的UI，更强大的搜索性能以及更多   
[小众软件-Aiverything - GPU加速的文件搜索&启动工具](https://meta.appinn.net/t/topic/66229)   
欢迎加入我们的QQ群，一起交流学习，或者水群聊聊天，交个朋友~：893463594   

![Aiverything-LOGO](https://raw.githubusercontent.com/panwangwin/aiverything-official-forum/refs/heads/main/logo-2.png)    
[![Aiverything](https://img.shields.io/badge/Try-Aiverything-blue?style=for-the-badge)](https://github.com/panwangwin/aiverything-official-forum)


## 编译该项目

---

- JDK >= 21  (项目使用了jdk21虚拟线程)
- Visual Studio 2022（C++ 生成工具 >= v143）
- maven >= 3.6.1
```bash
mvn clean compile package
```
- 编译后在target目录下将会得到File-Engine-Core.jar   
- 使用7zip（或其他压缩软件）打开File-Engine-Core.jar，将META-INF/versions/9/org/sqlite复制到根目录下的org/sqlite中，更新jar文件   
- 打开visual studio目录下的x64 Native Tools Command Prompt for VS 2022   
- 使用cd切换到target目录下
- 运行一下native image编译命令，需要使用[GraalVM](https://www.graalvm.org/downloads/)
```bash
native-image --no-fallback -Dorg.sqlite.lib.exportPath=./outDir -H:Path=./outDir -jar File-Engine-Core.jar -H:+JNI -R:MaxHeapSize=512M -R:MinHeapSize=32M -H:+UseCompressedReferences -R:MaxHeapFree=16777216
```
最后在outDir下将会得到File-Engine-Core.exe，sqlitejdbc.dll可以删除。

## 💖感谢以下项目：

- [gson](https://github.com/google/gson)
- [sqlite_jdbc](https://github.com/xerial/sqlite-jdbc)   
- [lombok](https://projectlombok.org/)   
- [TinyPinyin](https://github.com/promeG/TinyPinyin)
- [OpenCLWrapper](https://github.com/ProjectPhysX/OpenCL-Wrapper)
- [oshi](https://github.com/oshi/oshi)
- [jna](https://github.com/java-native-access/jna)
- [Javalin](https://javalin.io/)
