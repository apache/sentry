package org.apache.access.tests.e2e;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.access.core.Action;
import org.apache.access.core.Authorizable;
import org.apache.access.core.Subject;
import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;

public class SlowLocalGroupResourceAuthorizationProvider extends LocalGroupResourceAuthorizationProvider {

  static long sleepLengthSeconds = 30;
  private boolean hasSlept;

  public SlowLocalGroupResourceAuthorizationProvider(String resource) {
    super(resource);
  }
  @Override
  public boolean hasAccess(Subject subject, List<Authorizable> authorizableHierarchy,
      EnumSet<Action> actions) {
    if(!hasSlept) {
      hasSlept = true;
      try {
        TimeUnit.SECONDS.sleep(sleepLengthSeconds);
      } catch (InterruptedException exception) {
        throw new RuntimeException(exception);
      }
    }
    return super.hasAccess(subject, authorizableHierarchy, actions);
  }

  static void setSleepLengthInSeconds(long seconds) {
    sleepLengthSeconds = seconds;
  }
}
