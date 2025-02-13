@echo off

pushd %~dp0

docker-compose down -v

del /S /Q .\kafka\data

docker-compose up -d --build

popd

pause