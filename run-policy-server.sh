#!/bin/bash -u

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# XXX: replace this with your implementation
POLSRVR=MyPolicyServer

if [ ! -f src/$POLSRVR ] ; then
    echo "FATAL: $POLSRVR binary not built... exit"
    exit 1
fi

# Stop YARN....
sudo ydown

# Kill any currently running policy servers....
sudo killall $POLSRVR

# Purge old log files....
sudo rm -f /tmp/my_policy_server.log
sudo rm -f /tmp/my_policy_server.out

sudo rm -f /srv/yarn/results.txt

# Restart YARN....
sudo yup
echo ""
echo ""
sleep 0.1
# Start a new policy server daemon
echo "Starting policy server..."
export MY_CONFIG="$(cd `dirname $0` && pwd -P)/job-conf.json"
nohup src/$POLSRVR </dev/null 1>/tmp/my_policy_server.out 2>/tmp/my_policy_server.log &
echo "Redirecting stdout to /tmp/my_policy_server.out..."
echo "Redirecting stderr to /tmp/my_policy_server.log..."
sleep 0.1

echo ""
echo "OK - running in background"
echo "--------------------"
echo "!!! POLSRVR UP !!!"

exit 0
