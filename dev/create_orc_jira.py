#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import subprocess
import sys
import traceback

try:
    import jira.client

    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

# ASF JIRA access token
JIRA_ACCESS_TOKEN = os.environ.get("JIRA_ACCESS_TOKEN")
JIRA_API_BASE = "https://issues.apache.org/jira"


def fail(msg):
    print(msg)
    sys.exit(-1)


def run_cmd(cmd):
    print(cmd)
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).decode("utf-8")
    else:
        return subprocess.check_output(cmd.split(" ")).decode("utf-8")


def create_jira_issue(title):
    asf_jira = jira.client.JIRA(
        {"server": JIRA_API_BASE},
        token_auth=JIRA_ACCESS_TOKEN
    )

    issue_dict = {
        'project': {'key': 'ORC'},
        'summary': title,
        'description': '',
        'issuetype': {'name': 'Improvement'},
    }

    try:
        new_issue = asf_jira.create_issue(fields=issue_dict)
        return new_issue.key
    except Exception as e:
        fail("Failed to create JIRA issue: %s" % e)


def create_and_checkout_branch(jira_id):
    try:
        run_cmd("git checkout -b %s" % jira_id)
        print("Created and checked out branch: %s" % jira_id)
    except subprocess.CalledProcessError as e:
        fail("Failed to create branch %s: %s" % (jira_id, e))


def create_commit(jira_id, title):
    try:
        run_cmd(['git', 'commit', '-a', '-m', '%s: %s' % (jira_id, title)])
        print("Created a commit with message: %s: %s" % (jira_id, title))
    except subprocess.CalledProcessError as e:
        fail("Failed to create commit: %s" % e)


def main():
    if not JIRA_IMPORTED:
        fail("Could not find jira-python library. Run 'sudo pip3 install jira' to install.")

    if not JIRA_ACCESS_TOKEN:
        fail("The env-var JIRA_ACCESS_TOKEN is not set.")

    if len(sys.argv) < 2:
        fail("Usage: %s <JIRA title>" % sys.argv[0])

    title = sys.argv[1]
    print("Creating JIRA issue with title: %s" % title)

    jira_id = create_jira_issue(title)
    print("Created JIRA issue: %s" % jira_id)

    create_and_checkout_branch(jira_id)

    create_commit(jira_id, title)


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        traceback.print_exc()
        sys.exit(-1)
