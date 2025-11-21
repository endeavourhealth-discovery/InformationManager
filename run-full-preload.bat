@ECHO off
REM ========== CHECK/SET THESE CONFIGURATION ENTRIES ==========
SET "JAVA_VERSION=corretto-21.0.8"
SET "IMPORT_DATA=Z:\IdeaProjects\Endeavour\InformationManager\ImportData"
SET "TRUD_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
SET "Q_AUTH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

REM ========== SET REMAINING CONFIG BASED ON THE ABOVE ==========
SET "JAVA_HOME=%USERPROFILE%\.jdks\%JAVA_VERSION%"
SET "TRUD_DATA_DIR=%IMPORT_DATA%\TRUD"
SET "PRELOAD_TEMP=%IMPORT_DATA%\.tmp"
SET "GRAPHDB_BIN=%USERPROFILE%\AppData\Local\GraphD~1\app\bin"
SET "GRAPHDB_DATA=%USERPROFILE%\AppData\Roaming\GraphDB\data\repositories"
SET "GRAPHDB_START_CMD=%USERPROFILE%\AppData\Local\GraphD~1\GraphDB Desktop.exe"
SET "GRAPH_REPO=im"
SET "GRAPH_SERVER=http://localhost:7200/"
SET "Q_URL=https://api.apiqcodes.org/production"

REM ========== COMMAND LINE ARGUMENTS ==========
SET target=%1
SET additional=%2

IF NOT [%~3]==[] GOTO IncorrectArgs

IF NOT "%additional%"=="" (
  IF NOT "%additional%"=="smartlife" GOTO IncorrectArgs
)

IF "%target%"=="dev" (
  SET branch=develop
  GOTO Preload
) ELSE IF "%target%"=="live" (
  SET branch=main
  GOTO Preload
) ELSE (
  GOTO IncorrectArgs
)

:IncorrectArgs
  ECHO Incorrect arguments!
  ECHO Usage: %0 dev^|live ^<smartlife^>
  EXIT /B -1

:Preload
  ECHO Proceeding with preload for %target%
  IF NOT "%additional%"=="" ECHO (include Smartlife)

  ECHO Fetching ImportData
  IF EXIST ../ImportData/ (
    git -C ../ImportData checkout || exit /b %errorlevel%
    git -C ../ImportData pull || exit /b %errorlevel%
  ) ELSE (
    git clone https://github.com/endeavourhealth-discovery/ImportData ../ImportData || exit /b %errorlevel%
  )

  ECHO Fetching %target% IMAPI
  IF EXIST ../IMAPI/ (
    git -C ../IMAPI checkout %branch% || exit /b %errorlevel%
    git -C ../IMAPI pull origin %branch% || exit /b %errorlevel%
  ) ELSE (
    git clone https://github.com/endeavourhealth-discovery/IMAPI -b %branch% --single-branch ../IMAPI || exit /b %errorlevel%
  )

  pushd .
  cd ../IMAPI || exit /b %errorlevel%
  CALL gradlew assemble || exit /b %errorlevel%
  CALL gradlew publishToMavenLocal || exit /b %errorlevel%
  popd

  ECHO Fetching %target% InformationManager
  IF EXIST ../InformationManager/ (
    git -C ../IMAPI checkout || exit /b %errorlevel%
    git -C ../IMAPI pull || exit /b %errorlevel%
  ) ELSE (
    git clone https://github.com/endeavourhealth-discovery/InformationManager ../InformationManager || exit /b %errorlevel%
  )

  pushd .
  cd ../InformationManager || exit /b %errorlevel%
  CALL gradlew assemble || exit /b %errorlevel%
  popd

  ECHO Performing TRUD update
  "%JAVA_HOME%/bin/java" -jar Feeds/build/libs/Feeds-1.0-SNAPSHOT.jar %TRUD_API_KEY% "%TRUD_DATA_DIR%" || exit /b %errorlevel%

  ECHO Performing Preload
  "%JAVA_HOME%/bin/java" -Xmx14g -cp Transforms/build/libs/Transforms-1.0-SNAPSHOT-all.jar org.endeavourhealth.informationmanager.transforms.preload.Preload "source=%IMPORT_DATA%" "preload=%GRAPHDB_BIN%" "temp=%PRELOAD_TEMP%" privacy=0 "cmd=%GRAPHDB_START_CMD%" || exit /b %errorlevel%

  ECHO Shutting down graphdb
  timeout 5
  taskkill /f /im "GraphDB Desktop.exe"

  ECHO Zipping im (vanilla) repository
  pushd .
  cd /d %GRAPHDB_DATA%
  tar -a -v -cf im.zip im || exit /b %errorlevel%
  popd

  ECHO Restarting graphdb
  start "" "%GRAPHDB_START_CMD%" || exit /b %errorlevel%

  ECHO Waiting for startup
  :retry
  ECHO Pinging...
  curl --connect-timeout 1 127.0.0.1:7200 || goto Retry
  ECHO Connected!

  ECHO Filing Smartlife
  "%JAVA_HOME%/bin/java" -cp Transforms/build/libs/Transforms-1.0-SNAPSHOT-all.jar org.endeavourhealth.informationmanager.transforms.online.ImportApp %IMPORT_DATA% smartlifequery skiplucene privacy=3 || exit /b %errorlevel%

  ECHO Shutting down graphdb
  timeout 5
  taskkill /f /im "GraphDB Desktop.exe"

  ECHO Zipping im (smartlife) repository
  pushd .
  cd /d %GRAPHDB_DATA% || exit /b %errorlevel%
  tar -a -v -cf im-smartlife.zip im || exit /b %errorlevel%
  popd
