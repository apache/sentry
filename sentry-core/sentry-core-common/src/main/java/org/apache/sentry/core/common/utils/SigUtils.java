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

import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * This class, as sun.misc.SignalHandler, supports a single signal listener per each signal.
 * The same signal listener can be used to handle multiple signals, but setting a listener
 * for the same signal twice will replace an old listener with the new one.
 * method.
 */
public final class SigUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SigUtils.class);

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
    private final SigListener listener;
    SigHandler(SigListener listener) {
      this.listener = listener;
    }
    @Override
    public void handle(Signal sig) {
      if (sig != null) {
        listener.onSignal(sig.toString());
        // signalling is a rare yet important event - let's always log it
        LOGGER.info("Signal propagated: " + sig);
      } else {
        LOGGER.error("Internal Error: null signal received");
      }
    }
  }

  /**
   * Register signal listener for a specific signal.
   *
   * Only one listener per specific signal is supported. Calling this method
   * multiple times will keep overwriting a previously set listener.
   *
   * @NotNull sigName
   * @NotNull sigListener
   * @param sigName signal name, as in UNIX "kill" command, e.g. INT, USR1, etc, without "SIG" prefix
   * @param sigListener signal notification callback object
   * @throws IllegalArgumentException if invalid signal name or a signal is already handled by OS or JVM
   */
  public static void registerSigListener(String sigName, SigListener sigListener) {
    if (sigListener == null) {
      throw new IllegalArgumentException("NULL signal listener");
    }
    if (sigName == null) {
      throw new IllegalArgumentException("NULL signal name");
    }
    Signal.handle(new Signal(sigName), new SigHandler(sigListener));
    LOGGER.info("Signal Listener registered for signal " + sigName);
  }
}
