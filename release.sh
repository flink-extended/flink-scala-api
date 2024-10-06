#!/bin/bash
set -ex
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.18.1 -Dgpg.passphrase="<phrase>" test "; project scala-api; release with-defaults"
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.19.1 -Dgpg.passphrase="<phrase>" test "; project scala-api; release with-defaults"
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.20.0 -Dgpg.passphrase="<phrase>" test "; project scala-api; release with-defaults"
wait