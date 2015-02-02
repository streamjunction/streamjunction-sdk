package com.streamjunction.sdk;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import com.streamjunction.sdk.rmi.SJFrameworkConfigurationException;
import com.streamjunction.sdk.rmi.SJSpoutFramework;

public class RuntimeProperty {
	
	private static Timer timer = null;
	
	private synchronized static Timer getTimer() {
		if (timer == null) {
			timer = new Timer("RuntimeProperty", true);			
		}
		return timer;
	}
	
	private static void schedule(TimerTask timerTask, long delay, long interval) {
		getTimer().schedule(timerTask, delay, interval);
	}
	
	private static class PropertyConfig {
		
		private AtomicReference<String> value = new AtomicReference<String>(null);
		
		public PropertyConfig(final String propertyName) {
			schedule(new TimerTask() {				
				@Override
				public void run() {
					String p = null;
					try {
						p = SJSpoutFramework.Factory.getFramework().getProperty(propertyName);
					} catch (SJFrameworkConfigurationException e) {
						// may be running in test mode => try system property
						p = System.getProperty(propertyName);
					} catch (Exception e) {
						// ignore
					}
					value.set(p);					
				}
			}, 2500, 2500);
			String p = null;
			try {
				p = SJSpoutFramework.Factory.getFramework().getProperty(propertyName);
			} catch (SJFrameworkConfigurationException e) {
				// may be running in test mode => try system property
				p = System.getProperty(propertyName);
			} catch (Exception e) {
				// ignore
			}
			value.set(p);
		}
		
		public String getValue() {
			return value.get();
		}
		
	}
	
	private static Map<String, PropertyConfig> properties = new HashMap<String, RuntimeProperty.PropertyConfig>(); 

	public static String getProperty(String name) {
		PropertyConfig config;
		synchronized (properties) {
			config = properties.get(name);
			if (config == null) {
				config = new PropertyConfig(name);
				properties.put(name, config);
			}
		}
		return config.getValue();
	}

}
