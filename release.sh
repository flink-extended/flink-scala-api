#!/bin/bash
set -ex
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.20.0 -Dgpg.passphrase=$GPG_PASSPHRASE test "; project scala-api; release with-defaults"
wait