#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys, os, re, urllib2, base64, subprocess, tempfile, shutil
from optparse import OptionParser

tmp_dir = None
BASE_JIRA_URL = 'https://issues.apache.org/jira'

def execute(cmd, log=True, output_file=""):
  processes = list()
  if log:
    print "INFO: Executing %s" % (cmd)
  if len(output_file) > 0 :
    processes.append(subprocess.Popen(cmd, shell=True))
    processes.append(subprocess.Popen('tail -f ' + output_file, shell=True))
    if processes[0].poll() is None:
      return processes[0].wait()
  else:
    return subprocess.call(cmd, shell=True)

def jira_request(result, url, username, password, data, headers):
  request = urllib2.Request(url, data, headers)
  print "INFO: URL = %s, Username = %s, data = %s, headers = %s" % (url, username, data, str(headers))
  if username and password:
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)
  return urllib2.urlopen(request)

def jira_get_defect_html(result, defect, username, password):
  url = "%s/browse/%s" % (BASE_JIRA_URL, defect)
  return jira_request(result, url, username, password, None, {}).read()

def jira_get_defect(result, defect, username, password):
  url = "%s/rest/api/2/issue/%s" % (BASE_JIRA_URL, defect)
  return jira_request(result, url, username, password, None, {}).read()

def jira_post_comment(result, defect, branch, username, password):
  url = "%s/rest/api/2/issue/%s/comment" % (BASE_JIRA_URL, defect)
  body = [ "Here are the results of testing the latest attachment" ]
  body += [ "%s against %s." % (result.attachment, branch) ]
  body += [ "" ]
  if result._fatal:
    result._error = [ result._fatal ] + result._error
  if result._error:
    count = len(result._error)
    if count == 1:
      body += [ "{color:red}Overall:{color} -1 due to an error" ]
    else:
      body += [ "{color:red}Overall:{color} -1 due to %d errors" % (count) ]
  else:
    body += [ "{color:green}Overall:{color} +1 all checks pass" ]
  body += [ "" ]
  for error in result._error:
    body += [ "{color:red}ERROR:{color} %s" % (error.replace("\n", "\\n")) ]
  for info in result._info:
    body += [ "INFO: %s" % (info.replace("\n", "\\n")) ]
  for success in result._success:
    body += [ "{color:green}SUCCESS:{color} %s" % (success.replace("\n", "\\n")) ]
  if "BUILD_URL" in os.environ:
    body += [ "" ]
    body += [ "Console output: %sconsole" % (os.environ['BUILD_URL']) ]
  body += [ "" ]
  body += [ "This message is automatically generated." ]
  body = "{\"body\": \"%s\"}" % ("\\n".join(body))
  headers = {'Content-Type' : 'application/json'}
  response = jira_request(result, url, username, password, body, headers)
  body = response.read()
  if response.code != 201:
    msg = """Request for %s failed:
  URL = '%s'
  Code = '%d'
  Comment = '%s'
  Response = '%s'
    """ % (defect, url, response.code, comment, body)
    print "FATAL: %s" % (msg)
    sys.exit(1)

# hack (from hadoop) but REST api doesn't list attachments?
def jira_get_attachment(result, defect, username, password):
  html = jira_get_defect_html(result, defect, username, password)
  pattern = "(/secure/attachment/\d+/%s[\w\.\-]*\.(patch|txt|patch\.txt))" % (re.escape(defect))
  matches = []
  for match in re.findall(pattern, html, re.IGNORECASE):
    matches += [ match[0] ]
  if matches:
    matches.sort()
    return  "%s%s" % (BASE_JIRA_URL, matches.pop())
  return None

def git_cleanup():
  clean_rc = execute("git clean -d -f", False)
  if clean_rc != 0:
    print "ERROR: git clean failed"
  reset_rc = execute("git reset --hard HEAD", False)
  if reset_rc != 0:
    print "ERROR: git reset failed"
  return clean_rc + reset_rc

def git_checkout(result, branch):
  if git_cleanup() != 0:
    result.fatal("git cleanup")
  if execute("git fetch origin") != 0:
    result.fatal("git fetch failed")
  if execute("git checkout %s || git checkout -b %s origin/%s" % (branch, branch, branch)) != 0:
    result.fatal("git checkout %s failed" % (branch))
  if execute("git reset --hard origin/%s" % (branch)) != 0:
    result.fatal("git reset %s failed" % (branch))
  if execute("git merge --ff-only origin/%s" % (branch)):
    result.fatal("git merge failed")

def git_apply(result, cmd, patch_file, output_dir):
  output_file = "%s/apply.txt" % (output_dir)
  rc = execute("%s %s 1>%s 2>&1" % (cmd, patch_file, output_file), True, output_file)
  output = ""
  if os.path.exists(output_file):
    with open(output_file) as fh:
      output = fh.read()
  if output:
    print output
  if rc != 0:
    result.fatal("failed to apply patch (exit code %d):\n%s\n" % (rc, output))

def mvn_clean(result, mvn_repo, output_dir, mvn_profile):
  output_file = output_dir+'/clean.txt'
  rc = execute("mvn clean -Dmaven.repo.local=%s %s 1>%s 2>&1" % (mvn_repo, mvn_profile, output_file), True, output_file)
  if rc != 0:
    result.fatal("failed to clean project (exit code %d)" % (rc))

def mvn_install(result, mvn_repo, output_dir, mvn_profile):
  output_file = output_dir+'/install.txt'
  rc = execute("mvn install -U -DskipTests -Dmaven.repo.local=%s %s 1>%s 2>&1" % (mvn_repo, mvn_profile, output_file), True, output_file)
  if rc != 0:
    result.fatal("failed to build with patch (exit code %d)" % (rc))

def find_all_files(top):
    for root, dirs, files in os.walk(top):
        for f in files:
            yield os.path.join(root, f)

def mvn_test(result, mvn_repo, output_dir, mvn_profile):
  output_file = output_dir+'/test.txt'
  rc = execute("mvn verify --fail-at-end -Dmaven.repo.local=%s %s 1>%s 2>&1" % (mvn_repo, mvn_profile, output_file), True, output_file)
  if rc == 0:
    result.success("all tests passed")
  else:
    result.error("mvn test exited %d" % (rc))
    failed_tests = []
    for path in list(find_all_files(".")):
      file_name = os.path.basename(path)
      if file_name.startswith("TEST-") and file_name.endswith(".xml"):
        fd = open(path)
        for line in fd:
          if "<failure" in line or "<error" in line:
            matcher = re.search("TEST\-(.*).xml$", file_name)
            if matcher:
              failed_tests += [ matcher.groups()[0] ]
        fd.close()
    for failed_test in failed_tests:
      result.error("Failed: %s" % (failed_test))

class Result(object):
  def __init__(self):
    self._error = []
    self._info = []
    self._success = []
    self._fatal = None
    self.exit_handler = None
    self.attachment = "Not Found"
  def error(self, msg):
    self._error.append(msg)
  def info(self, msg):
    self._info.append(msg)
  def success(self, msg):
    self._success.append(msg)
  def fatal(self, msg):
    self._fatal = msg
    self.exit_handler()
    self.exit()
  def exit(self):
    git_cleanup()
    if self._fatal or self._error:
      if tmp_dir:
        print "INFO: output is located %s" % (tmp_dir)
      sys.exit(1)
    elif tmp_dir:
      shutil.rmtree(tmp_dir)
      sys.exit(0)

usage = "usage: %prog [options]"
parser = OptionParser(usage)
parser.add_option("--branch", dest="branch",
                  help="Local git branch to test against", metavar="master", default="master")
parser.add_option("--defect", dest="defect",
                  help="Defect name", metavar="SENTRY-1787")
parser.add_option("--file", dest="filename",
                  help="Test patch file", metavar="FILE")
parser.add_option("--run-tests", dest="run_tests",
                  help="Run Tests", action="store_true")
parser.add_option("--username", dest="username",
                  help="JIRA Username", metavar="USERNAME", default="hiveqa")
parser.add_option("--post-results", dest="post_results",
                  help="Post results to JIRA (only works in defect mode)", action="store_true")
parser.add_option("--password", dest="password",
                  help="JIRA Password", metavar="PASSWORD")
parser.add_option("--workspace", dest="workspace",
                  help="Jenkins workspace directory", metavar="DIR")
parser.add_option("--hive-authz2", dest="hive_authz2",
                  help="Test patch for Hive authz2", action="store_true")

(options, args) = parser.parse_args()
if not (options.defect or options.filename):
  print "FATAL: Either --defect or --file is required."
  sys.exit(1)

if options.defect and options.filename:
  print "FATAL: Both --defect and --file cannot be specified."
  sys.exit(1)

if options.post_results and not options.password:
  print "FATAL: --post-results requires --password"
  sys.exit(1)

if not options.workspace:
  print "FATAL: --workspace is required"
  sys.exit(1)

patch_cmd = "bash ./dev-support/smart-apply-patch.sh"
branch = options.branch
defect = options.defect
username = options.username
password = options.password
run_tests = options.run_tests
post_results = options.post_results
workspace = options.workspace
hive_authz2 = options.hive_authz2
result = Result()

def log_and_exit():
  if result._fatal:
    print "FATAL: %s" % (result._fatal)
  for error in result._error:
    print "ERROR: %s" % (error)
  for info in result._info:
    print "INFO: %s" % (info)
  for success in result._success:
    print "SUCCESS: %s" % (success)
  result.exit()

result.exit_handler = log_and_exit

if post_results:
  def post_jira_comment_and_exit():
    jira_post_comment(result, defect, branch, username, password)
    result.exit()
  result.exit_handler = post_jira_comment_and_exit

if workspace.endswith("/"):
  workspace = workspace[:-1]
mvn_repo = workspace + "/maven-repo"
output_dir = workspace + "/test-output"
if os.path.exists(mvn_repo):
  if not os.path.isdir(mvn_repo):
    shutil.rmtree(mvn_repo)
    os.mkdir(mvn_repo)
else:
  os.mkdir(mvn_repo)
if os.path.exists(output_dir):
  shutil.rmtree(output_dir)
os.mkdir(output_dir)

if defect:
  jira_json = jira_get_defect(result, defect, username, password)
  if '"Patch Available"' not in jira_json:
    print "ERROR: Defect %s not in patch available state" % (defect)
    sys.exit(1)
  attachment = jira_get_attachment(result, defect, username, password)
  if '"Hive V2"' in jira_json:
    print "INFO: Hive V2 is detected from Jira"
    hive_authz2 = True
  if not attachment:
    print "ERROR: No attachments found for %s" % (defect)
    sys.exit(1)
  result.attachment = attachment
  # parse branch info
  branchPattern = re.compile('/secure/attachment/\d+/%s(\.\d+)-(\S+)\.(patch|txt|patch.\txt)' % (re.escape(defect)))
  try:
    branchInfo = re.search(branchPattern,attachment)
    if branchInfo:
      branch = branchInfo.group(2)
      print "INFO: Branch info is detected from attachment name: " + branch
  except:
    branch = "master"
    print "INFO: Branch info is not detected from attachment name, use branch: " + branch
  patch_contents = jira_request(result, result.attachment, username, password, None, {}).read()
  patch_file = "%s/%s.patch" % (output_dir, defect)
  with open(patch_file, 'a') as fh:
    fh.write(patch_contents)
elif options.filename:
  patch_file = options.filename
else:
  print "ERROR: Reached unreachable code. Please report."
  sys.exit(1)

mvn_profile=""
mvn_clean(result, mvn_repo, output_dir, mvn_profile)
git_checkout(result, branch)
git_apply(result, patch_cmd, patch_file, output_dir)
mvn_install(result, mvn_repo, output_dir, mvn_profile)
if run_tests:
  mvn_test(result, mvn_repo, output_dir, mvn_profile)
else:
  result.info("patch applied and built but tests did not execute")
if hive_authz2:
  result.info("INFO: Test patch for Hive authz2")
  mvn_profile="-P-hive-authz1,hive-authz2"
  output_dir_v2 = output_dir + "/v2"
  os.mkdir(output_dir_v2)
  mvn_clean(result, mvn_repo, output_dir_v2, mvn_profile)
  mvn_install(result, mvn_repo, output_dir_v2, mvn_profile)
  if run_tests:
    mvn_test(result, mvn_repo, output_dir_v2, mvn_profile)
  else:
    result.info("patch applied and built but tests did not execute for Hive authz2")

result.exit_handler()
