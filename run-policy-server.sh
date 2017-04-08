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

if [ ! -f src/MyPolicyServer ] ; then
    echo "PolicyServer binary not found... exit"
    exit 1
fi

# Stop YARN....
sudo ydown

# Kill any currently running policy servers....
sudo killall MyPolicyServer

# Purge old logs...
sudo rm -f /tmp/my_policy_server.log
sudo rm -f /tmp/my_policy_server.out

sudo rm -f /srv/yarn/results.txt

# Restart YARN...
sudo yup

# Start our new policy server daemon
nohup src/MyPolicyServer </dev/null 1>/tmp/my_policy_server.out 2>/tmp/my_policy_server.log &
echo "Redirecting stdout to /tmp/my_policy_server.out..."
echo "Redirecting stderr to /tmp/my_policy_server.log..."
sleep 0.1

echo ""
echo "OK - policy server appears to be runnung..."
echo "--------------------"
echo "!!! POLSRVR UP !!!"

exit 0

