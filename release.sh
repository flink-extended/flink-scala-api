#!/bin/bash
set -ex
RELEASE_VERSION_BUMP=true sbt -Dgpg.passphrase=$GPG_PASSPHRASE test "; project scala-api; release with-defaults"
wait