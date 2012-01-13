BeginPackage["HBaseLink`", {"JLink`"}]

HBaseLink::usage = "HBaseLink[] is a reference to an open HBase connection"
OpenHBaseLink::usage = "OpenHBaseLink[config rules]"

HBaseListTables::usage = "HBaseListTables[link]"
HBaseListColumns::usage = "HBaseListColumns[link, \"table\"]"
HBaseDescribeTable::usage = "HBaseDescribeTable[link, \"table\"]"

HBaseSetSchema::usage = "HBaseSetSchema[link, \"table\", key -> {\"Decoder\", ...} ...] \
sets the decoding scheme for an HBase table."

HBaseGet::usage = "HBaseGet[link, \"table\", \"key\"]"

HBaseCount::usage = "HBaseCount[link, \"table\"]"

HBaseScan::usage = "HBaseScan[link, \"table\"]"

OpenHBaseLink::connfail = "Unable to establish connection with HBase."
HBaseLink::ischema = "Schema key must be of the form \"key\", {<family>, <qualifier>} or {<family>, <qualifier>, <start>, <stop>}"

Begin["`Private`"]
(* Implementation of the package *)

toBytesBinary[str_String] := JavaBlock[
    LoadJavaClass["org.apache.hadoop.hbase.util.Bytes", StaticsVisible -> True];
    Bytes`toBytesBinary[str]]

OpenHBaseLink[config : Rule[_String, _String]...] := Module[{hBaseLink},
    InstallJava[];
    LoadJavaClass["java.lang.System", StaticsVisible -> True];
        
    (* Check Java version *)
    With[{javaVersion = System`getProperty["java.version"]},
      If[ToExpression@StringTake[javaVersion, 3] < 1.6,
        die["HBaseLink` requires Java 6 or higher."];
      ]];
        
    (* Create HBaseLink java client *)
    hBaseLink = JavaNew["com.wolfram.hbaselink.HBaseLink", List @@@ {config}];
    If[hBaseLink === $Failed, Message[OpenHBaseLink::connfail]];
    HBaseLink[hBaseLink]]

getConf[h_HBaseLink] := h[[1]]@getConf[]
getCache[h_HBaseLink] := h[[1]]@getCache[]

getHBaseAdmin[h_HBaseLink] := JavaBlock[
    JavaNew["org.apache.hadoop.hbase.client.HBaseAdmin", getConf[h]]]

HBaseListTables[h_HBaseLink] := Module[{admin, tables},
    admin = getHBaseAdmin[h];
    tables = admin@listTables[];
    #@getNameAsString[] & /@ tables]

HBaseDescribeTable[h_HBaseLink, tablestr_String] := Module[{admin, table},
    admin = getHBaseAdmin[h];
    table = admin@getTableDescriptor[toBytesBinary[tablestr]];
    {
      "name" -> table@getNameAsString[],
      "families" -> ({
        "name" -> #@getNameAsString[], 
        "bloomFilter" -> #@getBloomFilterType[]@toString[],
        "replicationScope" -> #@getScope[],
        "compression" -> #@getCompression[]@toString[],
        "versions" -> #@getMaxVersions[],
        "timeToLive" -> #@getTimeToLive[],
        "blocksize" -> #@getBlocksize[],
        "inMemory" -> #@isInMemory[],
        "blockCache" -> #@isBlockCacheEnabled[]} & /@ table@getColumnFamilies[])
    }]

HBaseListColumns[h_HBaseLink, tablestr_String] := Module[{admin, tabledesc, columns},
    admin = getHBaseAdmin[h];
    tabledesc = admin@getTableDescriptor[toBytesBinary[tablestr]];
    columns = tabledesc@getColumnFamilies[];
    #@getNameAsString[] & /@ columns]

encodeHTableKey[table_, key_String] := key;

encodeHTableKey[table_, key_List] := Module[{array, i},
    LoadJavaClass["org.apache.hadoop.hbase.util.Bytes", StaticsVisible -> True];
    LoadJavaClass["java.lang.reflect.Array", StaticsVisible -> True];
    array = ReturnAsJavaObject[Array`newInstance[JavaNew["java.lang.Object"]@getClass[], Length[key]]];
    For[i = 0, i < Length[key], i += 1,
      Array`set[array, i, MakeJavaObject[key[[i + 1]]]]];
    Bytes`toStringBinary[table@getTranscoder[]@encode[array]]]

cacheContains[h_HBaseLink, tablestr_String] :=
    getCache[h]@containsKey[JavaNew["java.lang.String", tablestr]]

cachePut[h_HBaseLink, key_String, value_] :=
    getCache[h]@put[JavaNew["java.lang.String", key], value]

cacheGet[h_HBaseLink, key_String] :=
    getCache[h]@get[JavaNew["java.lang.String", key]]

cacheRemove[h_HBaseLink, key_String] :=
    getCache[h]@remove[JavaNew["java.lang.String", key]]

getHBaseHTable[h_HBaseLink, tablestr_String] := 
    If[cacheContains[h, tablestr],
      cacheGet[h, tablestr],
      With[{table = JavaNew["com.wolfram.hbaselink.HTable", getConf[h], toBytesBinary[tablestr]]},
        cachePut[h, tablestr, table];
        table]]

clearHBaseHTable[h_HBaseLink, tablestr_String] :=
    cacheRemove[h, tablestr]

HBaseSetSchema[h_HBaseLink, tablestr_String, schema___Rule] := 
    Module[
      {table, index, rule, key, transcoder},
      clearHBaseHTable[h, tablestr];
      table = getHBaseHTable[h, tablestr];
      
      For[index = 1, index <= Length[{schema}], index += 1,
        rule = {schema}[[index]];
        key = rule[[1]];
        
        transcoder = JavaNew["com.wolfram.hbaselink." <> rule[[2, 1]], Sequence @@ rule[[2, 2 ;;]]];
        If[transcoder === $Failed, Return[$Failed]];
        
        Which[
          MatchQ[key, _String] && key === "key",
          table@setTranscoder[transcoder],
        
          MatchQ[key, List[_String, _String]],
          table@setTranscoder[toBytesBinary[key[[1]]], toBytesBinary[key[[2]]], transcoder],
        
          MatchQ[key, List[_String, _String, _Integer, _Integer]],
          table@setTranscoder[toBytesBinary[key[[1]]], toBytesBinary[key[[2]]], key[[3]], key[[4]], transcoder],
          
          True,
          Message[HBaseLink::ischema];
          Return[$Failed];
        ]
      ]
    ]

Options[HBaseGet] = {
  "Columns" -> None,
  "Families" -> None,
  "Column" -> None,
  "Family" -> None,
  "Versions" -> 1,
  "TimeStamp" -> None,
  "TimeRange" -> None
}

HBaseGet[h_HBaseLink, tablestr_String, key_, opts : OptionsPattern[HBaseGet]] := 
  Module[{table, get},
    table = getHBaseHTable[h, tablestr];
    
    get = JavaNew["org.apache.hadoop.hbase.client.Get", toBytesBinary[encodeHTableKey[table, key]]];
    If[OptionValue["Families"] =!= None,
      get@addFamily[toBytesBinary[#]] & /@ OptionValue["Families"]];
    If[OptionValue["Columns"] =!= None,
      get@addColumn[toBytesBinary[#[[1]]], toBytesBinary[#[[2]]]] & /@ OptionValue["Columns"]];
    If[OptionValue["Family"] =!= None,
      get@addFamily[toBytesBinary[OptionValue["Family"]]]];
    If[OptionValue["Column"] =!= None,
      get@addColumn[toBytesBinary[OptionValue["Column"][[1]]], toBytesBinary[OptionValue["Column"][[2]]]]];
    
    get@setMaxVersions[OptionValue["Versions"]];
    If[OptionValue["TimeStamp"] =!= None,
      get@setTimeStamp[OptionValue["TimeStamp"]]];
    If[OptionValue["TimeRange"] =!= None,
      get@setTimeRange[OptionValue["TimeRange"][[1]], OptionValue["TimeRange"][[2]]]];
      
    table@getDecoded[get]]

Options[HBaseCount] = {
  "CachingRows" -> 1000
}

HBaseCount[h_HBaseLink, tablestr_String, opts : OptionsPattern[HBaseCount]] := 
  Module[{table},
    table = getHBaseHTable[h, tablestr];
    Monitor[table@countDecoded[OptionValue["CachingRows"]], Refresh[StringJoin[{
        "Count: ", ToString[table@getCurrentCount[]], "  Row: ", ToString[table@getCurrentRow[]]}], 
      UpdateInterval -> 0.5]]]

Options[HBaseScan] = {
  "StartRow" -> None,
  "StopRow" -> None,
  "Columns" -> None,
  "Families" -> None,
  "Column" -> None,
  "Family" -> None,
  "Versions" -> 1,
  "TimeStamp" -> None,
  "TimeRange" -> None,
  "Filter" -> None,
  "CacheBlocks" -> True,
  "Limit" -> All
}

HBaseScan[h_HBaseLink, tablestr_String, opts : OptionsPattern[HBaseScan]] :=
  Module[{table, scan, limit},
    table = getHBaseHTable[h, tablestr];
    
    scan = JavaNew["org.apache.hadoop.hbase.client.Scan"];
    If[OptionValue["StartRow"] =!= None,
      scan@setStartRow[toBytesBinary[encodeHTableKey[table, OptionValue["StartRow"]]]]];
    If[OptionValue["StopRow"] =!= None,
      scan@setStopRow[toBytesBinary[encodeHTableKey[table, OptionValue["StopRow"]]]]];
    
    If[OptionValue["Families"] =!= None,
      scan@addFamily[toBytesBinary[#]] & /@ OptionValue["Families"]];
    If[OptionValue["Columns"] =!= None,
      scan@addColumn[toBytesBinary[#[[1]]], toBytesBinary[#[[2]]]] & /@ OptionValue["Columns"]];
    If[OptionValue["Family"] =!= None,
      scan@addFamily[toBytesBinary[OptionValue["Family"]]]];
    If[OptionValue["Column"] =!= None,
      scan@addColumn[toBytesBinary[OptionValue["Column"][[1]]], toBytesBinary[OptionValue["Column"][[2]]]]];
    If[And @@ (OptionValue[#] === None & /@ {"Columns", "Families", "Column", "Family"}),
      scan@addFamily[toBytesBinary[#]] & /@ HBaseListColumns[h, tablestr]];

    If[OptionValue["Filter"] =!= None,
      scan@setFilter[OptionValue["Filter"]]];
    scan@setCacheBlocks[OptionValue["CacheBlocks"]];
    scan@setMaxVersions[OptionValue["Versions"]];
    If[OptionValue["TimeStamp"] =!= None,
      scan@setTimeStamp[OptionValue["TimeStamp"]]];
    If[OptionValue["TimeRange"] =!= None,
      scan@setTimeRange[OptionValue["TimeRange"][[1]], OptionValue["TimeRange"][[2]]]];
    
    table@setScan[scan];
    limit = If[OptionValue["Limit"] === All, -1, OptionValue["Limit"]];
    Monitor[table@scanDecoded[limit], Refresh[StringJoin[{
      "Count: ", ToString[table@getCurrentCount[]], "  Row: ", ToString[table@getCurrentRow[]]}], 
      UpdateInterval -> 0.5]]]

End[]

EndPackage[]

