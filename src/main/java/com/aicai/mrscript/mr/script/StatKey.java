package com.aicai.mrscript.mr.script;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/*
 * Stat Object
 */
public class StatKey {
	private String remote_addr;// 记录客户端的ip地址
	private String remote_user;// 记录客户端用户名称,忽略属性"-"
	private String time_local;// 记录访问时间与时区
	private String request;// 记录请求的url与http协议
	private String status;// 记录请求状态；成功是200
	private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
	private String http_referer;// 用来记录从那个页面链接访问过来的
	private String http_user_agent;// 记录客户浏览器的相关信息
	// "$uid" "$http_cookie"
	private String uid;// 记录UID
	private String cookie;// 记录Cookie
	private boolean valid = true;// 判断数据是否合法
	private String channel;
	private String cookieChannel;

	// 194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET
	// /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-"
	// "Mozilla/4.0 (compatible;)" - "1030000000091320"
	// "__c_uactiveat=1350108823468; __c_review=46; __c_last=1351061042906;
	// __c_visitor=1350104003730701; uid=103;
	// __utma=224849432.425128387.1339588739.1354606821.1354625898.809;
	// __utmb=224849432.13.10.1354625898; __utmc=224849432;
	// __utmz=224849432.1354108162.783.24.utmcsr=e.weibo.com|utmccn=(referral)|utmcmd=referral|utmcct=/2036070420/z7ixpvGv4;
	// sess=50b8f.796"

	public String getCookieChannel() {
		return cookieChannel;
	}

	public void setCookieChannel(String cookieChannel) {
		this.cookieChannel = cookieChannel;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	private static StatKey parser(String line) {
		System.out.println(line);
		StatKey Stat = new StatKey();
		String[] arr = line.split(" ");
		if (arr.length > 11) {
			Stat.setRemote_addr(arr[0]);
			Stat.setRemote_user(arr[1]);
			Stat.setTime_local(arr[3].substring(1));
			Stat.setRequest(arr[6]);
			Stat.setStatus(arr[8]);
			Stat.setBody_bytes_sent(arr[9]);
			Stat.setHttp_referer(arr[10]);

			if (arr.length > 12) {
				Stat.setHttp_user_agent(arr[11] + " " + arr[12]);
			} else {
				Stat.setHttp_user_agent(arr[11]);
			}

			if (Integer.parseInt(Stat.getStatus()) >= 400) {// 大于400，HTTP错误
				Stat.setValid(false);
			}

			String[] arr2 = line.split("\"");

			String str = "unknown";
			if (Stat.getRequest().contains("c=")) {
				int indexOf = line.indexOf("c=");
				str = line.substring(indexOf + 2, indexOf + 5);
			}
			Stat.setChannel(str);
			if (arr2 != null && arr2.length > 7) {
				Stat.setUid(arr2[7]);
				String ctr = arr2[9];
				Stat.setCookie(ctr);
				int indexOf = ctr.indexOf("c=");
				String cookiestr = ctr.substring(indexOf + 2, indexOf + 5);
				Stat.setCookieChannel(cookiestr);
			}

		} else {
			Stat.setValid(false);
		}
		return Stat;
	}

	/**
	 * 按page的pv分类
	 */
	public static StatKey filterPVs(String line) {
		StatKey Stat = parser(line);
		Set<String> pages = new HashSet<String>();
		pages.add("/about");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		pages.add("/hadoop-family-roadmap/");
		pages.add("/hadoop-hive-intro/");
		pages.add("/hadoop-zookeeper-intro/");
		pages.add("/hadoop-mahout-roadmap/");

		if (!pages.contains(Stat.getRequest())) {
			Stat.setValid(false);
		}
		return Stat;
	}

	/**
	 * 按page的独立ip分类
	 */
	public static StatKey filterIPs(String line) {
		StatKey Stat = parser(line);
		Set<String> pages = new HashSet<String>();
		pages.add("/about");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		pages.add("/hadoop-family-roadmap/");
		pages.add("/hadoop-hive-intro/");
		pages.add("/hadoop-zookeeper-intro/");
		pages.add("/hadoop-mahout-roadmap/");

		if (!pages.contains(Stat.getRequest())) {
			Stat.setValid(false);
		}

		return Stat;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}

	/**
	 * 按UV分类
	 */
	public static StatKey filterUV(String line) {
		return parser(line);
	}

	/**
	 * PV按浏览器分类
	 */
	public static StatKey filterBroswer(String line) {
		return parser(line);
	}

	/**
	 * PV按小时分类
	 */
	public static StatKey filterTime(String line) {
		return parser(line);
	}

	/**
	 * PV按访问域名分类
	 */
	public static StatKey filterDomain(String line) {
		return parser(line);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("valid:" + this.valid);
		sb.append("\nremote_addr:" + this.remote_addr);
		sb.append("\nremote_user:" + this.remote_user);
		sb.append("\ntime_local:" + this.time_local);
		sb.append("\nrequest:" + this.request);
		sb.append("\nstatus:" + this.status);
		sb.append("\nbody_bytes_sent:" + this.body_bytes_sent);
		sb.append("\nhttp_referer:" + this.http_referer);
		sb.append("\nhttp_user_agent:" + this.http_user_agent);
		sb.append("\nuid:" + this.uid);
		sb.append("\nhttp_cookie:" + this.cookie);
		return sb.toString();
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getRemote_user() {
		return remote_user;
	}

	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}

	public String getTime_local() {
		return time_local;
	}

	public Date getTime_local_Date() throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
		return df.parse(this.time_local);
	}

	public String getTime_local_Date_hour() throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
		return df.format(this.getTime_local_Date());
	}

	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}

	public String getHttp_referer() {
		return http_referer;
	}

	public String getHttp_referer_domain() {
		if (http_referer.length() < 8) {
			return http_referer;
		}

		String str = this.http_referer.replace("\"", "").replace("http://", "").replace("https://", "");
		return str.indexOf("/") > 0 ? str.substring(0, str.indexOf("/")) : str;
	}

	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public static void main(String args[]) {
		// String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET
		// /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\"
		// \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)
		// Chrome/29.0.1547.66 Safari/537.36\"";
		// System.out.println(line);
		String line = "192.168.5.161 - - [07/Aug/2015:09:31:22 +0800] \"GET /common/getTopList?c=b12&cateid=20000000 HTTP/1.1\" 200 1591 \"http://dev.aixuedai.com:10430/\" \"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36\" - \"1030000000091320\" \"c=b15; __c_uactiveat=1350108823468; __c_review=46; __c_last=1351061042906; __c_visitor=1350104003730701; uid=103; __utma=224849432.425128387.1339588739.1354606821.1354625898.809; __utmb=224849432.13.10.1354625898; __utmc=224849432; __utmz=224849432.1354108162.783.24.utmcsr=e.weibo.com|utmccn=(referral)|utmcmd=referral|utmcct=/2036070420/z7ixpvGv4; sess=50b8f.796\"";
		// String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET
		// /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\"
		// \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)
		// Chrome/29.0.1547.66 Safari/537.36\" - \"\"
		// \"__c_uactiveat=1350108823468; __c_review=46; __c_last=1351061042906;
		// __c_visitor=1350104003730701; uid=103;
		// __utma=224849432.425128387.1339588739.1354606821.1354625898.809;
		// __utmb=224849432.12.10.1354625898; __utmc=224849432;
		// __utmz=224849432.1354108162.783.24.utmcsr=e.weibo.com|utmccn=(referral)|utmcmd=referral|utmcct=/2036070420/z7ixpvGv4;
		// sess=50b8f.796\"";

		StatKey Stat = new StatKey();

		System.out.println(line.substring(line.indexOf("c=") + 2, line.indexOf("c=") + 5));
		String[] arr = line.split(" ");

		Stat.setRemote_addr(arr[0]);
		Stat.setRemote_user(arr[1]);
		Stat.setTime_local(arr[3].substring(1));
		Stat.setRequest(arr[6]);
		Stat.setStatus(arr[8]);
		Stat.setBody_bytes_sent(arr[9]);
		Stat.setHttp_referer(arr[10]);
		Stat.setHttp_user_agent(arr[11] + " " + arr[12]);
		String[] arr2 = line.split("\"");
		Stat.setUid(arr2[7]);
		Stat.setCookie(arr2[9]);

		String str = arr2[9];
		int indexOf = str.indexOf("c=");
		String cookiestr = str.substring(indexOf + 2, indexOf + 5);
		System.out.println("cookie channel is " + cookiestr);
		System.out.println(Stat);
		System.out.println("-----------------------------------------------");
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss", Locale.US);
			System.out.println(df.format(Stat.getTime_local_Date()));
			System.out.println(Stat.getTime_local_Date_hour());
			System.out.println(Stat.getHttp_referer_domain());
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
