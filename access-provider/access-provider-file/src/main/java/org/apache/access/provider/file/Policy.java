package org.apache.access.provider.file;

import com.google.common.collect.ImmutableSet;

public interface Policy {

  /**
   * Get permissions associated with a group. Returns Strings which can be resolved
   * by the caller. Strings are returned to separate the PolicyFile class from the
   * type of permissions used in a policy file. Additionally it's possible further
   * processing of the permissions is needed before resolving to a permission object.
   * @param group name
   * @return non-null immutable set of permissions
   */
  public abstract ImmutableSet<String> getPermissions(String group);

}