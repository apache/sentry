/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.common.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * This class facilitates handling signals in Sentry.
 * <p>
 * It defines its own callback interface to isolate the client code
 * from the dependency on sun.misc.*. The sun.misc.* package is not officially
 * supported, yet, per many indications, it is very unlikely to be dropped in
 * the future JDK releases. If needed it can be re-implemented using JNI,
 * though we'd rather not ever take this path.
 * <p>
 * This class relies on sun.misc.SignalHandler which registers signal handlers via static
 * method handle(). Therefore, SigUtils also only exposes a static method registerListener()
 * <p>
 * This class, as sun.misc.SignalHandler, supports a multiple signal listeners per each signal.
 * The same signal listener can be used to handle multiple signals.
 */
public final class SigUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SigUtils.class);
  private static Map<String, SigHandler> sigHandlerMap = new HashMap<>();

  private SigUtils() {} // to prevent class instantiation

  /**
   * interface to be implemented by a class that wants to be notified of a signal
   */
  public interface SigListener {
    void onSignal(String signalName);
  }

  /* private wrapper class around SignalHandler, to delegate signal handling
   * to a registered signal listener.
   */
  private static class SigHandler implements SignalHandler {
    private final Set<SigListener> listeners = new HashSet<>();
    SigHandler() {
    }
    public void addListener(SigListener sigListener){
      listeners.add(sigListener);
    }

    public void removeListener(SigListener sigListener){
      listeners.remove(sigListener);
    }

    @Override
    public void handle(Signal sig) {
      if (sig != null) {
        for (SigListener listener : listeners) {
          try {
            LOGGER.debug("Running Signal Handler {}.onSignal()", listener.getClass().getName());
            listener.onSignal(sig.toString());
          } catch (Exception e) {
            LOGGER.warn(String.format("Signal handler %s.onSignal() threw an exception:",
                listener.getClass().getName()), e);
          }
        }
        // signalling is a rare yet important event - let's always log it
        LOGGER.info("Signal propagated: " + sig);
      } else {
        LOGGER.error("Internal Error: null signal received");
      }
    }
  }

  /**
   * Adds a signal listener for a specific signal.
   *
   * Multiple listeners per specific signal are supported but execution order is indeterminate.
   *
   * @NotNull sigName
   * @NotNull sigListener
   * @param sigName signal name, as in UNIX "kill" command, e.g. INT, USR1, etc, without "SIG" prefix
   * @param sigListener signal notification callback object
   * @throws IllegalArgumentException if invalid signal name or a signal is already handled by OS or JVM
   */
  public static void registerSigListener(String sigName, SigListener sigListener) {
    if (StringUtils.isEmpty(sigName)) {
      throw new IllegalArgumentException("NULL signal name");
    }
    if (sigListener == null) {
      throw new IllegalArgumentException("NULL signal listener");
    }
    if (!sigHandlerMap.containsKey(sigName)){
      sigHandlerMap.put(sigName, new SigHandler());
    }
    SigHandler sigHandler = sigHandlerMap.get(sigName);
    sigHandler.addListener(sigListener);
    Signal.handle(new Signal(sigName), sigHandler);
    LOGGER.info("Signal Listener registered for signal " + sigName);
  }

  public static void unregisterSigListener(String sigName, SigListener sigListener){
    if (StringUtils.isEmpty(sigName)) {
      throw new IllegalArgumentException("NULL signal name");
    }
    if (sigListener == null) {
      throw new IllegalArgumentException("NULL signal listener");
    }
    if (!sigHandlerMap.containsKey(sigName)){
      sigHandlerMap.get(sigName).removeListener(sigListener);
    }
  }
}
