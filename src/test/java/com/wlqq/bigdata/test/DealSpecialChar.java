package com.wlqq.bigdata.test;

import java.util.regex.Pattern;

import com.alibaba.fastjson.JSONObject;

public class DealSpecialChar {
	public static void main(String[] args){
		String s = "{'class':'com.wlqq.etc.deposit.web.controller.LoginController','exception':{'class':'com.wlqq.base.exception.UserNotExistException','message':'User Not Found Exception!','stack':'com.wlqq.base.exception.UserNotExistException: User Not Found Exception!\n	at com.wlqq.http.sso.service.MainSessionServiceImpl.checkUserCredential(MainSessionServiceImpl.java:223)\n	at com.wlqq.http.sso.service.MainSessionServiceImpl.startSession(MainSessionServiceImpl.java:77)\n	at sun.reflect.GeneratedMethodAccessor49.invoke(Unknown Source)\n	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n	at java.lang.reflect.Method.invoke(Method.java:606)\n	at org.springframework.aop.support.AopUtils.invokeJoinpointUsingReflection(AopUtils.java:317)\n	at org.springframework.aop.framework.ReflectiveMethodInvocation.invokeJoinpoint(ReflectiveMethodInvocation.java:190)\n	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:157)\n	at org.springframework.remoting.support.RemoteInvocationTraceInterceptor.invoke(RemoteInvocationTraceInterceptor.java:78)\n	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:179)\n	at org.springframework.aop.framework.JdkDynamicAopProxy.invoke(JdkDynamicAopProxy.java:207)\n	at com.sun.proxy.$Proxy24.startSession(Unknown Source)\n	at sun.reflect.GeneratedMethodAccessor49.invoke(Unknown Source)\n	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n	at java.lang.reflect.Method.invoke(Method.java:606)\n	at com.caucho.hessian.server.HessianSkeleton.invoke(HessianSkeleton.java:155)\n	at org.springframework.remoting.caucho.HessianExporter.doInvoke(HessianExporter.java:223)\n	at org.springframework.remoting.caucho.HessianExporter.invoke(HessianExporter.java:138)\n	at org.springframework.remoting.caucho.HessianServiceExporter.handleRequest(HessianServiceExporter.java:66)\n	at org.springframework.web.servlet.mvc.HttpRequestHandlerAdapter.handle(HttpRequestHandlerAdapter.java:51)\n	at org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:943)\n	at org.springframework.web.servlet.DispatcherServlet.doService(DispatcherServlet.java:877)\n	at org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:966)\n	at org.springframework.web.servlet.FrameworkServlet.doPost(FrameworkServlet.java:868)\n	at javax.servlet.http.HttpServlet.service(HttpServlet.java:650)\n	at org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:842)\n	at javax.servlet.http.HttpServlet.service(HttpServlet.java:731)\n	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303)\n	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208)\n	at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52)\n	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241)\n	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208)\n	at org.tuckey.web.filters.urlrewrite.UrlRewriteFilter.doFilter(UrlRewriteFilter.java:350)\n	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241)\n	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208)\n	at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:88)\n	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107)\n	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241)\n	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208)\n	at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220)\n	at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122)\n	at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:505)\n	at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:170)\n	at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103)\n	at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116)\n	at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:423)\n	at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1079)\n	at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.java:620)\n	at org.apache.tomcat.util.net.AprEndpoint$SocketWithOptionsProcessor.run(AprEndpoint.java:2453)\n	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)\n	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)\n	at org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61)\n	at java.lang.Thread.run(Thread.java:745)'},'file_location':{'docker':'etc','host':'node7','ip':'192.168.1.7','path':'/var/logs/ulog/etc/etc.log','service':'etc'},'http_context':{'client_ip':'192.168.1.1','request_uuid':'4d089f3c-5e24-4f78-8a2e-718cb1eb5ab4'},'id':'bfc35033-c67e-42c7-9a06-528ee246e695','level':'ERROR','line':'61','message':'login error, {}','method':'login','sequence_number':11922,'thread_id':50,'thread_name':'catalina-exec-3','time':'2016-05-12 18:06:17.506+08:00','type':0,'version':1}";
		//System.out.println(s.replaceAll("[\\d\\w\\n\\t\\s\\r\\{\\}\":'\\$\\-!.,/<>\\[\\]\\(\\)\\+]", ""));
		//System.out.println(s.replaceAll("[^\\d\\w\\n\\t\\s\\r~`!@#\\$%\\^&\\*\\(\\)\\-_=\\+\\{\\[\\}\\]:;\"'<,>.\\?/]", ""));
		
		String json = "{\"f1\":\"bbb\"}";
//		json = json.replaceAll("\t", "\\\\t");
//		json = json.replaceAll("\n", "\\\\n");
		System.out.println(json.replaceAll("\\\\n", "\n"));
		json = json.replaceAll("([\\x00-\\x1f])", "\\\\$1");
//		JSONObject jo = JSONObject.parseObject(json);
//		jo.put("f1", jo.getString("f1").replaceAll("\t", "\\\\n"));
		//jo.put("f1", jo.getString("f1").replaceAll("([\\x00-\\x1f])", "\\'$1"));
		//String d=jo.toJSONString();
		System.out.println(json);
		//13510
		char b = (char)11;
		//char c = '\u0010';
		//json = "ac"+b+"d";
		System.out.println('\u000c'==(char)12);
//		System.out.println(json);
//		System.out.println("1"+c+"1");
		JSONObject jo = JSONObject.parseObject(json);
		String str = "a"+b+"a";
		System.out.println(str);
		jo.put("f2", str);
		System.out.println(jo.toJSONString());
		String m = jo.toJSONString();
		JSONObject o = new JSONObject();
		o.put("f1", str);
		System.out.println(o.toString());
		JSONObject j1 = JSONObject.parseObject(m);
		System.out.println(m.length());
		//System.out.println(json.replaceAll("([\\x00-\\x1f])", ""));
		
	}

}
