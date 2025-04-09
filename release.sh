#!/bin/bash
set -ex
RELEASE_VERSION_BUMP=true sbt -Dgpg.passphrase=$GPG_PASSPHRASE test "; release with-defaults"
wait