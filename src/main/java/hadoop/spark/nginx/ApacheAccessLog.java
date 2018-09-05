package hadoop.spark.nginx;


public class ApacheAccessLog {

	private String ipAddress;
	private String clientIdentd;
	private String dateTime;
	private String method;
	private String endpoint;
	private String protocol;
	private String responseCode;
	private String contentSize;

    public ApacheAccessLog(){

    }
	public ApacheAccessLog(String ipAddress, String dateTime, String method, String endpoint, String protocol, String responseCode, String contentSize, String clientIdentd) {
		this.ipAddress = ipAddress;
		this.dateTime = dateTime;
		this.method = method;
		this.endpoint = endpoint;
		this.protocol = protocol;
		this.responseCode = responseCode;
		this.contentSize = contentSize;
		this.clientIdentd = clientIdentd;
	}

	/**
	 * get ipAddress value
	 * @return the ipAddress
	 */
	public String getIpAddress() {
		return ipAddress;
	}
	/**
	 * set ipAddress value
	 * @param ipAddress the ipAddress to set
	 */
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	/**
	 * get clientIdentd value
	 * @return the clientIdentd
	 */
	public String getClientIdentd() {
		return clientIdentd;
	}
	/**
	 * set clientIdentd value
	 * @param clientIdentd the clientIdentd to set
	 */
	public void setClientIdentd(String clientIdentd) {
		this.clientIdentd = clientIdentd;
	}
	/**
	 * get dateTime value
	 * @return the dateTime
	 */
	public String getDateTime() {
		return dateTime;
	}
	/**
	 * set dateTime value
	 * @param dateTime the dateTime to set
	 */
	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}
	/**
	 * get method value
	 * @return the method
	 */
	public String getMethod() {
		return method;
	}
	/**
	 * set method value
	 * @param method the method to set
	 */
	public void setMethod(String method) {
		this.method = method;
	}
	/**
	 * get endpoint value
	 * @return the endpoint
	 */
	public String getEndpoint() {
		return endpoint;
	}
	/**
	 * set endpoint value
	 * @param endpoint the endpoint to set
	 */
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	/**
	 * get protocol value
	 * @return the protocol
	 */
	public String getProtocol() {
		return protocol;
	}
	/**
	 * set protocol value
	 * @param protocol the protocol to set
	 */
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	/**
	 * get responseCode value
	 * @return the responseCode
	 */
	public String getResponseCode() {
		return responseCode;
	}
	/**
	 * set responseCode value
	 * @param responseCode the responseCode to set
	 */
	public void setResponseCode(String responseCode) {
		this.responseCode = responseCode;
	}
	/**
	 * get contentSize value
	 * @return the contentSize
	 */
	public String getContentSize() {
		return contentSize;
	}
	/**
	 * set contentSize value
	 * @param contentSize the contentSize to set
	 */
	public void setContentSize(String contentSize) {
		this.contentSize = contentSize;
	}
	/** (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ApacheAccessLog [ipAddress=" + ipAddress + ", clientIdentd=" + clientIdentd
				+ ", dateTime=" + dateTime + ", method=" + method + ", endpoint=" + endpoint + ", protocol=" + protocol
				+ ", responseCode=" + responseCode + ", contentSize=" + contentSize + "]";
	}
}
