package org.apache.sentry.binding.hive.conf;

public class InvalidConfigurationException extends Exception
{
	private static final long serialVersionUID = 1L;

	//Parameterless Constructor
    public InvalidConfigurationException() {}

    //Constructor that accepts a message
    public InvalidConfigurationException(String message)
    {
      super(message);
    }
 }
