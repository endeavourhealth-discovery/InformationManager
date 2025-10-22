@ECHO off
SET JAVA_HOME="C:\Users\<!!USER!!>\.jdks\corretto-21.0.8"
SET TRUD_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SET TRUD_DATA_DIR="Z:\IdeaProjects\Endeavour\InformationManager\ImportData\TRUD"
SET PRELOAD_SOURCE="Z:\IdeaProjects\Endeavour\InformationManager\ImportData"
SET PRELOAD_TEMP="Z:\IdeaProjects\Endeavour\InformationManager\ImportData\.tmp"
SET GRAPHDB_BIN="C:\Users\<!!USER!!>\AppData\Local\GraphD~1\app\bin"
SET "GRAPHDB_DATA=C:\Users\<!!USER!!>\AppData\Roaming\GraphDB\data\repositories"
SET "GRAPHDB_START_CMD=C:\Users\<!!USER!!>\AppData\Local\GraphD~1\GraphDB Desktop.exe"
SET GRAPH_REPO=im
SET GRAPH_SERVER=http://localhost:7200/
SET Q_AUTH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SET Q_URL=https://api.apiqcodes.org/production

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
  EXIT /B 1

:Preload
  ECHO Proceeding with preload for %target%
  IF NOT "%additional%"=="" ECHO (include Smartlife)

  ECHO Fetching ImportData
  IF EXIST ../ImportData/ (
    git -C ../ImportData checkout
    git -C ../ImportData pull
  ) ELSE (
    git clone https://github.com/endeavourhealth-discovery/ImportData ../ImportData
  )

  ECHO Fetching %target% IMAPI
  IF EXIST ../IMAPI/ (
    git -C ../IMAPI checkout %branch%
    git -C ../IMAPI pull origin %branch%
  ) ELSE (
    git clone https://github.com/endeavourhealth-discovery/IMAPI -b %branch% --single-branch ../IMAPI
  )

  pushd .
  cd ../IMAPI
  CALL gradlew assemble
  CALL gradlew publishToMavenLocal
  popd

  ECHO Fetching %target% InformationManager
  IF EXIST ../InformationManager/ (
    git -C ../IMAPI checkout
    git -C ../IMAPI pull
  ) ELSE (
    git clone https://github.com/endeavourhealth-discovery/InformationManager ../InformationManager
  )

  pushd .
  cd ../InformationManager
  CALL gradlew assemble
  popd

  ECHO Performing TRUD update
  "%JAVA_HOME%/bin/java" -jar Feeds/build/libs/Feeds-1.0-SNAPSHOT.jar %TRUD_API_KEY% "%TRUD_DATA_DIR%"

  ECHO Performing Preload
  "%JAVA_HOME%/bin/java" -cp Transforms/build/libs/Transforms-1.0-SNAPSHOT-all.jar org.endeavourhealth.informationmanager.transforms.preload.Preload "source=%PRELOAD_SOURCE%" "preload=%GRAPHDB_BIN%" "temp=%PRELOAD_TEMP%" privacy=0 "cmd=%GRAPHDB_START_CMD%"

  ECHO Shutting down graphdb
  timeout 5
  taskkill /f /im "GraphDB Desktop.exe"

  ECHO Zipping im (vanilla) repository
  pushd .
  cd /d %GRAPHDB_DATA%
  tar -a -v -cf im.zip im
  popd

  ECHO Restarting graphdb
  start "" "%GRAPHDB_START_CMD%"

  ECHO Waiting for startup
  :retry
  ECHO Pinging...
  curl --connect-timeout 1 127.0.0.1:7200 || goto Retry
  ECHO Connected!

  ECHO Filing Smartlife
  "%JAVA_HOME%/bin/java" -cp Transforms/build/libs/Transforms-1.0-SNAPSHOT-all.jar org.endeavourhealth.informationmanager.transforms.online.ImportApp %PRELOAD_SOURCE% smartlifequery skiplucene privacy=3

  ECHO Shutting down graphdb
  timeout 5
  taskkill /f /im "GraphDB Desktop.exe"

  ECHO Zipping im (smartlife) repository
  pushd .
  cd /d %GRAPHDB_DATA%
  tar -a -v -cf im-smartlife.zip im
  popd
