package com.sbsa.benchmark;

public class Endpoint {
	protected Object connection;
	protected Object session;
	protected Object endpoint;
	protected EndPointType endPointType;
	protected DeliveryMode deliveryMode;
	
	public Endpoint(){
		
	}
	
	public void setEndpoint(EndPointType endPointType, DeliveryMode deliveryMode, Object connection, Object session, Object endpoint) {
		this.endPointType = endPointType;
		this.deliveryMode = deliveryMode;
		this.connection = connection;
		this.session = session;
		this.endpoint = endpoint;
	}
	
	public Object getConnection(){
		return this.connection;
	}
	
	public Object getSession(){
		return this.session;
	}
	
	public Object getEndpoint(){
		return this.endpoint;
	}
	
	public EndPointType getEndPointType(){
		return this.endPointType;
	}
	
	public DeliveryMode getDeliveryMode() {
		return this.deliveryMode;
	}
}
