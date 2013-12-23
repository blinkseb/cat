#!/bin/bash

pushd `dirname ${BASH_SOURCE[0]}` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null

export PATH=$SCRIPTPATH/bin:$PATH
export PYTHONPATH=$SCRIPTPATH/cat:$PYTHONPATH
