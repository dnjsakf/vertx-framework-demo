@echo off

pushd %~dp0

docker-compose down -v

del /S /Q %CD%\kafka\data

docker-compose build --progress=plain

docker-compose -p dms-kafka up -d 

popd

pause