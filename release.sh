#!/bin/bash
set -ex
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.16.3 test 'release with-defaults'
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.17.2 test 'release with-defaults'
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.18.0 test 'release with-defaults'
wait