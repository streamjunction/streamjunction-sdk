package com.streamjunction.sdk.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpEntity;

public class CannotSubmitTopologyException extends Exception {
	
	private static final long serialVersionUID = -7738091479203620764L;
	
	private int statusCode;
	private String reason;
	private byte[] body;

	public CannotSubmitTopologyException(String message, int statusCode,
			String reason, HttpEntity entity) {
		super(message);
		
		this.statusCode = statusCode;
		this.reason = reason;
		
		ByteArrayOutputStream s = new ByteArrayOutputStream();
		
		if (entity != null) {
			InputStream in;
			try {
				in = entity.getContent();
			} catch (IllegalStateException ex) {
				in = null;
			} catch (IOException ex) {
				in = null;
			}

			if (in != null) {
				try {
					byte[] b = new byte[4096];

					int r;

					while ((r = in.read(b)) != -1) {
						s.write(b, 0, r);
					}
				} catch (IOException e) {
					// do nothing
				} finally {
					try {
						in.close();
					} catch (IOException e) {					
					}
				}
			}
		}
		
		this.body = s.toByteArray();
		
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getReason() {
		return reason;
	}

	public byte[] getBody() {
		return body;
	}

}
