HBaseLink is a _Mathematica_ package for reading from (and eventually writing to) HBase tables.

Here are some examples of what it can do:

	In[1]:= Needs["HBaseLink`"]
	
	
	(* Provide the HBase zookeeper quorum and clientPort to create the link *)
	In[2]:= link = OpenHBaseLink[
		"hbase.zookeeper.quorum" -> "hadoop1lx.wolfram.com,hadoop2lx.wolfram.com,hadoop3lx.wolfram.com,hadoop4lx.wolfram.com,hadoop5lx.wolfram.com", 
		"hbase.zookeeper.property.clientPort" -> "2181"]
	
	Out[2]= HBaseLink[<<JavaObject[com.wolfram.hbaselink.HBaseLink]>>]
	
	
	(* List tables in HBase *)
	In[3]:= HBaseListTables[link] // TableForm
	
	Out[3]//TableForm= bin_clickthroughs_hourly
	                   bin_cookie_visitors_hourly
	                   bin_host_webstats_day_intervals
	                   bin_host_webstats_hour_intervals
	                   bin_hostua_visitors_hourly
	                   bin_page_page_views_hourly
	                   bin_path_webstats_day_intervals
	                   bin_path_webstats_hour_intervals
	                   bin_query_webstats_day_intervals
	                   bin_query_webstats_hour_intervals
	                   bin_referrers_args_daily
	                   bin_referrers_hourly
	                   bin_request_args_hourly
	                   bin_site_page_views_hourly
	                   bin_url_webstats_hour_intervals
	                   build_status
	                   cookie_session
	                   hostua_session
	                   remote_session
	                   request
	                   shad
	
	
	(* Scan a single record from cookie_session *)
	In[4]:= HBaseScan[link, "cookie_session", "Limit" -> 1]
	
	Out[4]= {
		{L\xCB\xA6P169.231.13.43.1288412012191129, 
			{request,\x00\x00\x00\x00,1320284791934,www.wolframalpha.com:L\xCB\xA6P:colo4a-webprd3.wolfram.com:\x00\x00\x00\x00\x00\x00\x05k},
			{request,\x00\x00\x00\x01,1320284791934,www.wolframalpha.com:L\xCB\xA6Y:colo4a-webprd3.wolfram.com:\x00\x00\x00\x00\x00\x003\x86},
			{request,\x00\x00\x00\x02,1320284791934,www.wolframalpha.com:L\xCB\xA6t:colo4a-webprd3.wolfram.com:\x00\x00\x00\x00\x00\x00\xD1\xD5},
			{request,\x00\x00\x00\x03,1320284791934,www.wolframalpha.com:L\xCB\xA6\x8A:colo4a-webprd3.wolfram.com:\x00\x00\x00\x00\x00\x01-E},
			{summary,depth,1320284791934,\x03\x00\x00\x00\x04},
			{summary,duration,1320284791934,\x03\x00\x00\x00:},
			{summary,end_time,1320284791934,\x03L\xCB\xA6\x8A},
			{summary,end_url,1320284791934,\x07\x00\x00\x00\xD9http://www.wolframalpha.com/input/popup.txt?i=integrate%20-5(1+e^x)^(1/2)&src=http://www4a.wolframalpha.com/Calculate/MSP/MSP80819d0d39difiifc08000055a7f051cdc725dg?MSPStoreType=image/gif&s=40&w=449&h=1162&id=pod_0100},
			{summary,referrer,1320284791934,\x07\x00\x00\x00^http://www.wolframalpha.com/input/?i=derive+4x%5E2%2B2x%2Bxy%3D4+when+dy%284%29+y%284%29%3D-17},
			{summary,start_time,1320284791934,\x03L\xCB\xA6P},
			{summary,start_url,1320284791934,\x07\x00\x00\x00Nhttp://www.wolframalpha.com/input/?i=integrate+-5%281%2Be%5Ex%29%5E%281%2F2%29},
			{summary,user_id,1320284791934,\x07\x00\x00\x00\x1E169.231.13.43.1288412012191129}
		}
	}
	
	(* 
		The data is broken down as follows: 
			| Timestamp  | String                         |
			| L\xCB\xA6P | 169.231.13.43.1288412012191129 |
		
			{String , 4 Byte Integer  , Timestamp    , String              :Integer   :String                    :Long}
			{request, \x00\x00\x00\x00, 1320284791934, www.wolframalpha.com:L\xCB\xA6P:colo4a-webprd3.wolfram.com:\x00\x00\x00\x00\x00\x00\x05k}
			
			Notice that in this case, not only is the data encoded (String:Integer:String:Long), but so also is the column qualifier (4 Byte Integer).
	*)
	
	
	(* Set the schema for cookie_session *)
	In[5]:= HBaseSetSchema[link, "cookie_session",
		"Key" -> {"PackedBinary", "lA*"}, (* For decoding the key which is an Integer followed by an arbitrary length String *)
		"Qualifiers" -> {
			"request" -> {"PackedBinary", "l", True} (* For decoding the Column Family 'request' qualifiers, which are integers *)
		},
		"Columns" -> {
			{"request"} -> {"PackedBinary", "Y:*lx:Y:*q"}, (* For decoding request values which are String:Integer:String:Long *)
			{"summary"} -> {"TypedBytes", True} (* For decoding Column Family summary values which are all in TypedBytes format *)
		}
	]
	
	
	(* Get the data from cookie_session again (it will be decoded according to the schema provided) *)
	In[6]:= HBaseScan[link, "cookie_session", "Limit" -> 1]
	
	Out[6]= {
		{
			{1288414800, "169.231.13.43.1288412012191129"}, 
			{"request", 0, 1320284791934, {"www.wolframalpha.com", 1288414800, "colo4a-webprd3.wolfram.com", 1387}}, 
			{"request", 1, 1320284791934, {"www.wolframalpha.com", 1288414809, "colo4a-webprd3.wolfram.com", 13190}}, 
			{"request", 2, 1320284791934, {"www.wolframalpha.com", 1288414836, "colo4a-webprd3.wolfram.com", 53717}}, 
			{"request", 3, 1320284791934, {"www.wolframalpha.com", 1288414858, "colo4a-webprd3.wolfram.com", 77125}}, 
			{"summary", "depth", 1320284791934, 4}, 
			{"summary", "duration", 1320284791934, 58}, 
			{"summary", "end_time", 1320284791934, 1288414858}, 
			{"summary", "end_url", 1320284791934, "http://www.wolframalpha.com/input/popup.txt?i=integrate%20-5(1+e^x)^(1/2)&src=http://www4a.wolframalpha.com/Calculate/MSP/MSP80819d0d39difiifc08000055a7f051cdc725dg?MSPStoreType=image/gif&s=40&w=449&h=1162&id=pod_0100"}, 
			{"summary", "referrer", 1320284791934, "http://www.wolframalpha.com/input/?i=derive+4x%5E2%2B2x%2Bxy%3D4+when+dy%284%29+y%284%29%3D-17"}, 
			{"summary", "start_time", 1320284791934, 1288414800}, 
			{"summary", "start_url", 1320284791934, "http://www.wolframalpha.com/input/?i=integrate+-5%281%2Be%5Ex%29%5E%281%2F2%29"}, 
			{"summary", "user_id", 1320284791934, "169.231.13.43.1288412012191129"}
		}
	}
	
	
	(* Let's try getting by Key *)
	In[7]:= HBaseScan[link, "request", "Limit" -> 1]
	
	Out[7]= {
		{api.wolframalpha.com:MG\xA1a:colo4b-webprd3.wolfram.com:\x00\x00\x00\x00\x00\x00\x01\xA8,
			{http, agent ,1298836466983, \x07\x00\x00\x00\x18Wolfram Android App/null},
			{http, cookie, 1298836466983, \x07\x00\x00\x00\x1F192.148.117.79.1296540001674563},
			{http, host_or_ip, 1298836466983, \x07\x00\x00\x00\x0E192.148.117.79},
			{http, machine, 1298836466983, \x07\x00\x00\x00\x1Acolo4b-webprd3.wolfram.com},
			{http, method, 1298836466983, \x07\x00\x00\x00\x03GET},
			{http, offset, 1298836466983, \x04\x00\x00\x00\x00\x00\x00\x01\xA8},
			{http, protocol, 1298836466983, \x07\x00\x00\x00\x04http},
			{http, query, 1298836466983, \x07\x00\x00\x01\x05appid=3H4296-5YPAGQUJK7&input=whats+the+australian+presedent&format=image,plaintext,sound&async=0.25&scantimeout=1.0&latlong=-28.87140582,153.05061412&sidebarlinks=true&reinterpret=true&width=792&maxwidth=1192&device=Android&sig=2E931182A65D2865E736E2454CDD1F0D},
			{http, referrer, 1298836466983, \x07\x00\x00\x00\x01-},
			{http, status, 1298836466983, \x03\x00\x00\x00\xC8},
			{http, timestamp, 1298836466983, \x03MG\xA1a},
			{http, uri, 1298836466983, \x07\x00\x00\x00\x0D/v1/query.jsp},
			{http, virtual_host, 1298836466983, \x07\x00\x00\x00\x14api.wolframalpha.com},
			{session, cookie, 1298834572442, MG\xA1a192.148.117.79.1296540001674563},
			{session, hostua, 1298836466983, MG\xA1a192.148.117.79:Wolfram Android App/null}
		}
	}
	
	
	(* Set the schema for it... *)
	In[8]:= HBaseSetSchema[link, "request",
		"Key" -> {"PackedBinary", "Y:*lx:Y:*q"},
		"Columns" -> {
			{"http"} -> {"TypedBytes", True},
			{"session"} -> {"PackedBinary", "lA*"}
		}
	]
	
	
	(* Do a get -- key will be automatically encoded to bytes using the schema! *)
	In[9]:= HBaseGet[link, "request", {"www.wolframalpha.com", 1288414809, "colo4a-webprd3.wolfram.com", 13190}]
	
	Out[9]= {
		{www.wolframalpha.com, 1288414809, colo4a-webprd3.wolfram.com, 13190},
		{http, agent, 1320285197136, Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB6.5; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)},
		{http, cookie, 1320285197136, 169.231.13.43.1288412012191129},
		{http, host_or_ip, 1320285197136, 169.231.13.43},
		{http, machine, 1320285197136, colo4a-webprd3.wolfram.com},
		{http, method, 1320285197136, GET},
		{http, offset, 1320285197136, 13190},
		{http, protocol, 1320285197136, http},
		{http, query, 1320285197136, compTO=true&i=integrate+-5(1%2be%5ex)%5e(1%2f2)},
		{http, referrer, 1320285197136, http://www.wolframalpha.com/input/?i=integrate+-5%281%2Be%5Ex%29%5E%281%2F2%29},
		{http, status, 1320285197136, 200},
		{http, timestamp, 1320285197136, 1288414809},
		{http, uri, 1320285197136, /input/timeout.jsp},
		{http, virtual_host, 1320285197136, www.wolframalpha.com},
		{session, cookie, 1320284783299, {1288414800, 169.231.13.43.1288412012191129}},
		{session, hostua, 1320285197138, {1288414800, 169.231.13.43:Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB6.5; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)}}
	}

Still much needs to be implemented. Error messages are non-existent. Puts and other administrative functions would be very nice. Partial encodings of keys to allow for range scans would be great. Also some smart server side filter classes that are parameterized by valid wild cards on keys and perhaps ranges for integers.