#!/bin/bash
set -ex
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.15.4 test 'release with-defaults'
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.16.2 test 'release with-defaults'
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.17.1 test 'release with-defaults'
wait