package com.streamjunction.sdk.log;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.streamjunction.sdk.RuntimeProperty;

/**
 * Debug operation configured at runtime. To turn on debug output you 
 * need to set the property "sjdebug" in the UI console for the given project.
 * 
 */
public class Debug implements Filter {
	
	private static Logger logger = LoggerFactory.getLogger("SJDebug");
	
	private final String label;
	
	private final Renderer renderer;
	
	public static interface Renderer extends Serializable {
		
		public String render(String label, TridentTuple tuple);
		
	}
	
	public Debug(String label) {
		this(label, new DefaultRenderer());
	}
	
	public Debug(String label, Renderer renderer) {
		this.label = label;
		this.renderer = renderer;
	}
	
	private static class DefaultRenderer implements Renderer {

		@Override
		public String render(String label, TridentTuple tuple) {
			return "["+label+"] "+Joiner.on("; ").join(
					Lists.transform(tuple, new Function<Object, Object>() {
						@Override
						public Object apply(Object input) {
							if (input != null) {
								return input;
							} else {
								return "null";
							}
						}
					}));
		}
		
	}
	
	private static boolean isDebugging() {
		return "true".equals(RuntimeProperty.getProperty("sjdebug"));
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		if (isDebugging()) {
			logger.info(renderer.render(label, tuple));
		}
		return true;
	}

	public static void log(String text, Throwable e) {
		if (isDebugging()) {
			if (e != null) {
				logger.info(text, e);
			} else {
				logger.info(text);
			}
		}
	}
	
	public static void log(String text) {
		log(text, null);
	}

}
