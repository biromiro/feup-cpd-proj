@echo off
REM Script to ease running the project in windows

set "JAVA_BIN=C:\Users\PedroGoncaloCorreia\.jdks\openjdk-18.0.1.1\bin"
set "OUT_DIR=out\production\assign2"
set "SRC_DIR=src"
set "SERVER_MAIN=Store"
set "CLIENT_MAIN=TestClient"

IF [%1]==[] (
    echo "Usage: run.cmd <rmiregister|compile|server|client>"
    exit /b 1
)
IF %1==rmiregistry (
    echo "Starting RMI Registry..."
    echo "%JAVA_BIN%\rmiregistry.exe" "-J-Djava.class.path=%OUT_DIR%/"
    "%JAVA_BIN%\rmiregistry.exe" "-J-Djava.class.path=%OUT_DIR%/"
    exit /b 1
)
IF %1==compile (
    echo "Compiling..."
    echo "%JAVA_BIN%\javac.exe" -d "%OUT_DIR%" "%SRC_DIR%/*.java"
    "%JAVA_BIN%\javac.exe" -d "%OUT_DIR%" %SRC_DIR%/*.java
    exit /b 1
)
IF %1==server (
    echo "Starting Server..."
    echo "%JAVA_BIN%\javac.exe" -d "%OUT_DIR%" "%SRC_DIR%/*.java"
    "%JAVA_BIN%\javac.exe" -d "%OUT_DIR%" %SRC_DIR%/*.java
    echo "%JAVA_BIN%\java.exe" -classpath "%OUT_DIR%" "-Djava.rmi.server.codebase=file:%OUT_DIR%/" "%SERVER_MAIN%" %2 %3 %4 %5 %6 %7 %8 %9
    "%JAVA_BIN%\java.exe" -classpath "%OUT_DIR%" "-Djava.rmi.server.codebase=file:%OUT_DIR%/" "%SERVER_MAIN%" %2 %3 %4 %5 %6 %7 %8 %9

    exit /b 1
)
IF %1==client (
    echo "Starting Client..."
    echo "%JAVA_BIN%\javac.exe" -d "%OUT_DIR%" "%SRC_DIR%/*.java"
    "%JAVA_BIN%\javac.exe" -d "%OUT_DIR%" %SRC_DIR%/*.java
    echo "%JAVA_BIN%\java.exe" -classpath "%OUT_DIR%" "%CLIENT_MAIN%" %2 %3 %4 %5 %6 %7 %8 %9
    "%JAVA_BIN%\java.exe" -classpath "%OUT_DIR%" "%CLIENT_MAIN%" %2 %3 %4 %5 %6 %7 %8 %9
    exit /b 1
)

echo "Usage: run.cmd <rmiregister|compile|server|client>"

