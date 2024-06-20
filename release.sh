#!/bin/bash
set -ex
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.17.2 test 'release with-defaults'
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.18.1 test 'release with-defaults'
RELEASE_VERSION_BUMP=true sbt -DflinkVersion=1.19.0 test 'release with-defaults'
wait