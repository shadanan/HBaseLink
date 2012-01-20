(* Mathematica Test File *)

<<HBaseLink`
link = OpenHBaseLink[
  "hbase.zookeeper.quorum" -> "hadoop1lx.wolfram.com,hadoop2lx.wolfram.com,hadoop3lx.wolfram.com,hadoop4lx.wolfram.com,hadoop5lx.wolfram.com", 
  "hbase.zookeeper.property.clientPort" -> "2181"]

Test[
    HBaseListColumns[link, "bin_clickthroughs_hourly"],
    {"counts"},
	TestID -> "HBaseLink-20120116-H4C9O6"]