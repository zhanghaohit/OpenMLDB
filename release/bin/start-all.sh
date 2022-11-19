#! /usr/bin/env bash

# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

home="$(cd "`dirname "$0"`"/..; pwd)"
. $home/conf/openmldb-env.sh
. $home/bin/init.sh

# if in cluster mode, start zk
if [[ -n "${OPENMLDB_MODE}" && ${OPENMLDB_MODE} = "cluster" ]]; then
  # start zk if OPENMLDB_USE_EXISTING_ZK_CLUSTER is not true
  if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
    cd $home
    bin/start-zk.sh
    sleep 5
  fi
fi

# Start Tablets
bin/start-tablets.sh

# Start Nameservers
bin/start-nameservers.sh

# Start Apiservers
bin/start-apiservers.sh

# if in cluster mode, start taskmanager
if [[ -n "${OPENMLDB_MODE}" && ${OPENMLDB_MODE} = "cluster" ]]; then
  # start taskmanager
  cd $home
  bin/start-taskmanagers.sh
fi

echo "OpenMLDB start success"
