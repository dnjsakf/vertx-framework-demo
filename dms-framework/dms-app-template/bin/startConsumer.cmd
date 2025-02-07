@echo off

set "WORK_DIR=%~dp0\.."

pushd %WORK_DIR%

set "TARGET_HOME=%CD%\target"
set "JAVA_CMD=C:\dev\java\openjdk-17.0.3.0.6\bin\java.exe"

%JAVA_CMD% -cp %TARGET_HOME%\dms-app-template-1.0.0-bin.jar com.dms.apps.FlumeApp

popd

pause
